/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.bootstrap;

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.Version;
import org.elasticsearch.common.PidFile;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.log4j.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalSettingsPreparer;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;

/**
 * Internal startup code.
 */
final class Bootstrap {

    private static volatile Bootstrap INSTANCE;

    private volatile Node node;
    private final CountDownLatch keepAliveLatch = new CountDownLatch(1);
    //keepAliveThread 其实这个线程相当于一个heartbeat，用以来表示ES进程是否还活着
    private final Thread keepAliveThread;

    /** creates a new instance */
    Bootstrap() {
        keepAliveThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    //线程启动之后就进入到await状态
                    keepAliveLatch.await();
                } catch (InterruptedException e) {
                    // bail out
                }
            }
        }, "elasticsearch[keepAlive/" + Version.CURRENT + "]");
        keepAliveThread.setDaemon(false);
        // keep this thread alive (non daemon thread) until we shutdown
        // 在程序退出时触发该Hook（注：退出是指ctrl+c或者kill -15，但如果用kill -9 那是没办法的），在该Hook中会对之前的线程做countDown操作
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                keepAliveLatch.countDown();
            }
        });
    }
    
    /** initialize native resources */
    public static void initializeNatives(Path tmpFile, boolean mlockAll, boolean seccomp, boolean ctrlHandler) {
        final ESLogger logger = Loggers.getLogger(Bootstrap.class);
        
        // check if the user is running as root, and bail
        // 检查其他的一些外部资源；首先不能用root启动ES，否则就会强制关闭
        if (Natives.definitelyRunningAsRoot()) {
            if (Boolean.parseBoolean(System.getProperty("es.insecure.allow.root"))) {
                logger.warn("running as ROOT user. this is a bad idea!");
            } else {
                throw new RuntimeException("don't run elasticsearch as root.");
            }
        }
        
        // enable secure computing mode
        if (seccomp) {
            Natives.trySeccomp(tmpFile);
        }
        
        // mlockall if requested
        if (mlockAll) {
            if (Constants.WINDOWS) {
               Natives.tryVirtualLock();
            } else {
               Natives.tryMlockall();
            }
        }

        // listener for windows close event
        if (ctrlHandler) {
            Natives.addConsoleCtrlHandler(new ConsoleCtrlHandler() {
                @Override
                public boolean handle(int code) {
                    if (CTRL_CLOSE_EVENT == code) {
                        logger.info("running graceful exit on windows");
                        Bootstrap.stop();
                        return true;
                    }
                    return false;
                }
            });
        }

        // force remainder of JNA to be loaded (if available).
        try {
            JNAKernel32Library.getInstance();
        } catch (Throwable ignored) {
            // we've already logged this.
        }

        // init lucene random seed. it will use /dev/urandom where available:
        StringHelper.randomId();
    }

    static void initializeProbes() {
        // Force probes to be loaded
        ProcessProbe.getInstance();
        OsProbe.getInstance();
    }

    private void setup(boolean addShutdownHook, Settings settings, Environment environment) throws Exception {
        // 对于mlockall的配置我们其实是建议设为true的，因为这样可以保障ES不会去占用其他系统的内存、swap等资源。
        // 强烈建议把bootstrap.mlockall设为true，这个值设为true，可能会让节点启动的时候比较慢，但是保证了节点加入集群后的稳定性。
        // 在5.x版本中，已经改成了bootstrap.memory_lock
        initializeNatives(environment.tmpFile(),
                          settings.getAsBoolean("bootstrap.mlockall", false),
                          settings.getAsBoolean("bootstrap.seccomp", true),
                          settings.getAsBoolean("bootstrap.ctrlhandler", true));

        // initialize probes before the security manager is installed
        // 初始化两种probes，分别是ProcessProbe和Osprobe，这两个probe将会提供给ES的stats api所需要的一些ES进程和OS层面的信息
        initializeProbes();

        //增加shutdown的hook，当集群shutdown的时候，会主动调用node的close方法，即让本node安全退出。
        if (addShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    if (node != null) {
                        node.close();
                    }
                }
            });
        }

        // look for jar hell
        JarHell.checkJarHell();

        // install SM after natives, shutdown hooks, etc.
        setupSecurity(settings, environment);

        // We do not need to reload system properties here as we have already applied them in building the settings and
        // reloading could cause multiple prompts to the user for values if a system property was specified with a prompt
        // placeholder
        // 接下来才是对于node的配置的初始化，这部分的配置来自配置文件
        Settings nodeSettings = Settings.settingsBuilder()
                .put(settings)
                .put(InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING, true)
                .build();

        // 通过nodeSettings创建nodeBuilder
        NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder().settings(nodeSettings);
        // 而node的初始化就是通过nodeSettings来创建的
        node = nodeBuilder.build();
    }
    
    /** 
     * option for elasticsearch.yml etc to turn off our security manager completely,
     * for example if you want to have your own configuration or just disable.
     */
    // TODO: remove this: http://www.openbsd.org/papers/hackfest2015-pledge/mgp00005.jpg
    static final String SECURITY_SETTING = "security.manager.enabled";
    /**
     * option for elasticsearch.yml to fully respect the system policy, including bad defaults
     * from java.
     */
    // TODO: remove this hack when insecure defaults are removed from java
    static final String SECURITY_FILTER_BAD_DEFAULTS_SETTING = "security.manager.filter_bad_defaults";

    private void setupSecurity(Settings settings, Environment environment) throws Exception {
        if (settings.getAsBoolean(SECURITY_SETTING, true)) {
            Security.configure(environment, settings.getAsBoolean(SECURITY_FILTER_BAD_DEFAULTS_SETTING, true));
        }
    }

    @SuppressForbidden(reason = "Exception#printStackTrace()")
    private static void setupLogging(Settings settings, Environment environment) {
        try {
            Class.forName("org.apache.log4j.Logger");
            LogConfigurator.configure(settings, true);
        } catch (ClassNotFoundException e) {
            // no log4j
        } catch (NoClassDefFoundError e) {
            // no log4j
        } catch (Exception e) {
            sysError("Failed to configure logging...", false);
            e.printStackTrace();
        }
    }

    private static Environment initialSettings(boolean foreground) {
        Terminal terminal = foreground ? Terminal.DEFAULT : null;
        return InternalSettingsPreparer.prepareEnvironment(EMPTY_SETTINGS, terminal);
    }

    private void start() {
        node.start();
        keepAliveThread.start();
    }

    static void stop() {
        try {
            Releasables.close(INSTANCE.node);
        } finally {
            INSTANCE.keepAliveLatch.countDown();
        }
    }

    /**
     * This method is invoked by {@link Elasticsearch#main(String[])}
     * to startup elasticsearch.
     */
    //真正的初始化在Bootstrap的init中，首先是对command的解析，并将参数置于system property中
    static void init(String[] args) throws Throwable {
        // Set the system property before anything has a chance to trigger its use
        System.setProperty("es.logger.prefix", "");

        BootstrapCLIParser bootstrapCLIParser = new BootstrapCLIParser();
        CliTool.ExitStatus status = bootstrapCLIParser.execute(args);

        if (CliTool.ExitStatus.OK != status) {
            System.exit(status.status());
        }

        INSTANCE = new Bootstrap();

        //从command来判断是不是后台启动，日志的初始化，然后写入pid。
        boolean foreground = !"false".equals(System.getProperty("es.foreground", System.getProperty("es-foreground")));
        // handle the wrapper system property, if its a service, don't run as a service
        if (System.getProperty("wrapper.service", "XXX").equalsIgnoreCase("true")) {
            foreground = false;
        }

        Environment environment = initialSettings(foreground);
        Settings settings = environment.settings();
        setupLogging(settings, environment);
        checkForCustomConfFile();

        if (environment.pidFile() != null) {
            PidFile.create(environment.pidFile(), true);
        }

        if (System.getProperty("es.max-open-files", "false").equals("true")) {
            ESLogger logger = Loggers.getLogger(Bootstrap.class);
            logger.info("max_open_files [{}]", ProcessProbe.getInstance().getMaxFileDescriptorCount());
        }

        // warn if running using the client VM
        // 获取JVM信息，如果是使用client vm则会给出提示 （建议使用server vm，毕竟ES需要很大的资源，可以用-server来强制使用server vm）
        if (JvmInfo.jvmInfo().getVmName().toLowerCase(Locale.ROOT).contains("client")) {
            ESLogger logger = Loggers.getLogger(Bootstrap.class);
            logger.warn("jvm uses the client vm, make sure to run `java` with the server vm for best performance by adding `-server` to the command line");
        }

        try {
            if (!foreground) {
                Loggers.disableConsoleLogging();
                closeSystOut();
            }

            // fail if using broken version
            // 默认是开启jvm 版本检查的，如果不检查可能会导致es数据的损坏。针对不同公司的jvm vendor会有不同的处理方式，
            JVMCheck.check();

            INSTANCE.setup(true, settings, environment);

            INSTANCE.start();

            if (!foreground) {
                closeSysError();
            }
        } catch (Throwable e) {
            // disable console logging, so user does not see the exception twice (jvm will show it already)
            if (foreground) {
                Loggers.disableConsoleLogging();
            }
            ESLogger logger = Loggers.getLogger(Bootstrap.class);
            if (INSTANCE.node != null) {
                logger = Loggers.getLogger(Bootstrap.class, INSTANCE.node.settings().get("name"));
            }
            // HACK, it sucks to do this, but we will run users out of disk space otherwise
            if (e instanceof CreationException) {
                // guice: log the shortened exc to the log file
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                PrintStream ps = new PrintStream(os, false, "UTF-8");
                new StartupError(e).printStackTrace(ps);
                ps.flush();
                logger.error("Guice Exception: {}", os.toString("UTF-8"));
            } else {
                // full exception
                logger.error("Exception", e);
            }
            // re-enable it if appropriate, so they can see any logging during the shutdown process
            if (foreground) {
                Loggers.enableConsoleLogging();
            }
            
            throw e;
        }
    }

    @SuppressForbidden(reason = "System#out")
    private static void closeSystOut() {
        System.out.close();
    }

    @SuppressForbidden(reason = "System#err")
    private static void closeSysError() {
        System.err.close();
    }

    @SuppressForbidden(reason = "System#err")
    private static void sysError(String line, boolean flush) {
        System.err.println(line);
        if (flush) {
            System.err.flush();
        }
    }

    private static void checkForCustomConfFile() {
        String confFileSetting = System.getProperty("es.default.config");
        checkUnsetAndMaybeExit(confFileSetting, "es.default.config");
        confFileSetting = System.getProperty("es.config");
        checkUnsetAndMaybeExit(confFileSetting, "es.config");
        confFileSetting = System.getProperty("elasticsearch.config");
        checkUnsetAndMaybeExit(confFileSetting, "elasticsearch.config");
    }

    private static void checkUnsetAndMaybeExit(String confFileSetting, String settingName) {
        if (confFileSetting != null && confFileSetting.isEmpty() == false) {
            ESLogger logger = Loggers.getLogger(Bootstrap.class);
            logger.info("{} is no longer supported. elasticsearch.yml must be placed in the config directory and cannot be renamed.", settingName);
            System.exit(1);
        }
    }
}
