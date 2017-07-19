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

package org.elasticsearch.client.transport;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.Sets;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.liveness.LivenessRequest;
import org.elasticsearch.action.admin.cluster.node.liveness.LivenessResponse;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.FutureTransportResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 *
 */
public class TransportClientNodesService extends AbstractComponent {

    private final TimeValue nodesSamplerInterval;

    private final long pingTimeout;

    private final ClusterName clusterName;

    private final TransportService transportService;

    private final ThreadPool threadPool;

    private final Version minCompatibilityVersion;

    private final Headers headers;

    // nodes that are added to be discovered
    // client初始化时addTransportAddress配置的node(代表配置文件中主动加入的节点)
    private volatile List<DiscoveryNode> listedNodes = Collections.emptyList();

    private final Object mutex = new Object();

    // 和服务端建立的node(代表参与请求的节点)
    private volatile List<DiscoveryNode> nodes = Collections.emptyList();
    // 没有建立连接的node(过滤掉的不能进行请求处理的节点)
    private volatile List<DiscoveryNode> filteredNodes = Collections.emptyList();

    private final AtomicInteger tempNodeIdGenerator = new AtomicInteger();

    private final NodeSampler nodesSampler;

    private volatile ScheduledFuture nodesSamplerFuture;

    private final AtomicInteger randomNodeGenerator = new AtomicInteger();

    private final boolean ignoreClusterName;

    private volatile boolean closed;

    @Inject
    public TransportClientNodesService(Settings settings, ClusterName clusterName, TransportService transportService,
                                       ThreadPool threadPool, Headers headers, Version version) {
        super(settings);
        this.clusterName = clusterName;
        this.transportService = transportService;
        this.threadPool = threadPool;
        this.minCompatibilityVersion = version.minimumCompatibilityVersion();
        this.headers = headers;

        this.nodesSamplerInterval = this.settings.getAsTime("client.transport.nodes_sampler_interval", timeValueSeconds(5));
        this.pingTimeout = this.settings.getAsTime("client.transport.ping_timeout", timeValueSeconds(5)).millis();
        this.ignoreClusterName = this.settings.getAsBoolean("client.transport.ignore_cluster_name", false);

        if (logger.isDebugEnabled()) {
            logger.debug("node_sampler_interval[" + nodesSamplerInterval + "]");
        }

        // 设置嗅探器类型
        if (this.settings.getAsBoolean("client.transport.sniff", false)) {
            // 嗅探同一集群中的所有节点
            /**
             * 我们先来看看SniffNodesSampler，这意味着client会主动发现集群里的其他节点；
             * 在sample中client会去ping listedNodes和nodes中所有节点，默认ping的interval为5s；
             * 如果ping的node在nodes list里面，意味着是要真正建立连接的node，则创建fully connct；
             * 如果不在则创建light connect。(这里的light是指只创建ping连接，fully则会创建前一篇文章所说的那五种连接)。
             * 然后对这些node发送一个获取其state的请求，获取集群所有的dataNodes，对这些nodes经过再次确认后就放入nodes中。
             */
            this.nodesSampler = new SniffNodesSampler();
        } else {
            // 只关注配置文件配置的节点
            // SimpleNodeSampler：ping listedNodes中的所有node，区别在于这里创建的都是light connect；
            /**
             * 同样是ping listedNodes中的所有node，区别在于这里创建的都是light connect
             * 对这些node发送一个TransportLivenessAction的请求，这个请求会返回一个自发现的node info
             * 把这个返回结果中真实的node加入nodes，如果返回时空，仍然会加入nodes，因为可能目标node还没有完成初始化还获取不到信息。
             */
            this.nodesSampler = new SimpleNodeSampler();
        }
        /**
         * 以上sampler
         * 可以看到两者的最大不同之处在于nodes列表里面的node,
         * 也就是SimpleNodeSampler让集群中的某些个配置的节点，专门用于接受用户请求。SniffNodesSampler的话，所有节点都会参与负载。
         */
        /**
         * 那么发送数据的时候会选择哪一个呢？其实都在execute方法里面： 随即轮询
         * 其实关键的就一句话DiscoveryNode node = nodes.get((index) % nodes.size());通过Round robin来生成发送的node，以达到负载均衡的效果。
         */
        this.nodesSamplerFuture = threadPool.schedule(nodesSamplerInterval, ThreadPool.Names.GENERIC, new ScheduledNodeSampler());
    }

    public List<TransportAddress> transportAddresses() {
        List<TransportAddress> lstBuilder = new ArrayList<>();
        for (DiscoveryNode listedNode : listedNodes) {
            lstBuilder.add(listedNode.address());
        }
        return Collections.unmodifiableList(lstBuilder);
    }

    public List<DiscoveryNode> connectedNodes() {
        return this.nodes;
    }

    public List<DiscoveryNode> filteredNodes() {
        return this.filteredNodes;
    }

    public List<DiscoveryNode> listedNodes() {
        return this.listedNodes;
    }

    public TransportClientNodesService addTransportAddresses(TransportAddress... transportAddresses) {
        synchronized (mutex) {
            if (closed) {
                throw new IllegalStateException("transport client is closed, can't add an address");
            }
            List<TransportAddress> filtered = new ArrayList<>(transportAddresses.length);
            for (TransportAddress transportAddress : transportAddresses) {
                boolean found = false;
                for (DiscoveryNode otherNode : listedNodes) {
                    if (otherNode.address().equals(transportAddress)) {
                        found = true;
                        logger.debug("address [{}] already exists with [{}], ignoring...", transportAddress, otherNode);
                        break;
                    }
                }
                if (!found) {
                    filtered.add(transportAddress);
                }
            }
            if (filtered.isEmpty()) {
                return this;
            }
            List<DiscoveryNode> builder = new ArrayList<>();
            builder.addAll(listedNodes());
            for (TransportAddress transportAddress : filtered) {
                DiscoveryNode node = new DiscoveryNode("#transport#-" + tempNodeIdGenerator.incrementAndGet(), transportAddress, minCompatibilityVersion);
                logger.debug("adding address [{}]", node);
                builder.add(node);
            }
            listedNodes = Collections.unmodifiableList(builder);
            nodesSampler.sample();
        }
        return this;
    }

    public TransportClientNodesService removeTransportAddress(TransportAddress transportAddress) {
        synchronized (mutex) {
            if (closed) {
                throw new IllegalStateException("transport client is closed, can't remove an address");
            }
            List<DiscoveryNode> builder = new ArrayList<>();
            for (DiscoveryNode otherNode : listedNodes) {
                if (!otherNode.address().equals(transportAddress)) {
                    builder.add(otherNode);
                } else {
                    logger.debug("removing address [{}]", otherNode);
                }
            }
            listedNodes = Collections.unmodifiableList(builder);
            nodesSampler.sample();
        }
        return this;
    }

    public <Response> void execute(NodeListenerCallback<Response> callback, ActionListener<Response> listener) {
        List<DiscoveryNode> nodes = this.nodes;
        ensureNodesAreAvailable(nodes);
        // auto_increment round-robbin
        int index = getNodeNumber();
            RetryListener<Response> retryListener = new RetryListener<>(callback, listener, nodes, index);
            // Client做负载均衡 nodes.get((index + 0) % nodes.size());
            DiscoveryNode node = nodes.get((index) % nodes.size());
            try {
            callback.doWithNode(node, retryListener);
        } catch (Throwable t) {
            //this exception can't come from the TransportService as it doesn't throw exception at all
            listener.onFailure(t);
        }
    }

    public static class RetryListener<Response> implements ActionListener<Response> {
        private final NodeListenerCallback<Response> callback;
        private final ActionListener<Response> listener;
        private final List<DiscoveryNode> nodes;
        private final int index;

        private volatile int i;

        public RetryListener(NodeListenerCallback<Response> callback, ActionListener<Response> listener, List<DiscoveryNode> nodes, int index) {
            this.callback = callback;
            this.listener = listener;
            this.nodes = nodes;
            this.index = index;
        }

        @Override
        public void onResponse(Response response) {
            listener.onResponse(response);
        }

        @Override
        public void onFailure(Throwable e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof ConnectTransportException) {
                int i = ++this.i;
                if (i >= nodes.size()) {
                    listener.onFailure(new NoNodeAvailableException("None of the configured nodes were available: " + nodes, e));
                } else {
                    try {
                        callback.doWithNode(nodes.get((index + i) % nodes.size()), this);
                    } catch(final Throwable t) {
                        // this exception can't come from the TransportService as it doesn't throw exceptions at all
                        listener.onFailure(t);
                    }
                }
            } else {
                listener.onFailure(e);
            }
        }


    }

    public void close() {
        synchronized (mutex) {
            if (closed) {
                return;
            }
            closed = true;
            FutureUtils.cancel(nodesSamplerFuture);
            for (DiscoveryNode node : nodes) {
                transportService.disconnectFromNode(node);
            }
            for (DiscoveryNode listedNode : listedNodes) {
                transportService.disconnectFromNode(listedNode);
            }
            nodes = Collections.emptyList();
        }
    }

    private int getNodeNumber() {
        int index = randomNodeGenerator.incrementAndGet();
        if (index < 0) {
            index = 0;
            randomNodeGenerator.set(0);
        }
        return index;
    }

    private void ensureNodesAreAvailable(List<DiscoveryNode> nodes) {
        if (nodes.isEmpty()) {
            String message = String.format(Locale.ROOT, "None of the configured nodes are available: %s", this.listedNodes);
            throw new NoNodeAvailableException(message);
        }
    }

    abstract class NodeSampler {
        public void sample() {
            synchronized (mutex) {
                if (closed) {
                    return;
                }
                doSample();
            }
        }

        protected abstract void doSample();

        /**
         * validates a set of potentially newly discovered nodes and returns an immutable
         * list of the nodes that has passed.
         */
        protected List<DiscoveryNode> validateNewNodes(Set<DiscoveryNode> nodes) {
            for (Iterator<DiscoveryNode> it = nodes.iterator(); it.hasNext(); ) {
                DiscoveryNode node = it.next();
                if (!transportService.nodeConnected(node)) {
                    try {
                        logger.trace("connecting to node [{}]", node);
                        transportService.connectToNode(node);
                    } catch (Throwable e) {
                        it.remove();
                        logger.debug("failed to connect to discovered node [" + node + "]", e);
                    }
                }
            }

            return Collections.unmodifiableList(new ArrayList<>(nodes));
        }

    }

    class ScheduledNodeSampler implements Runnable {
        @Override
        public void run() {
            try {
                nodesSampler.sample();
                if (!closed) {
                    nodesSamplerFuture = threadPool.schedule(nodesSamplerInterval, ThreadPool.Names.GENERIC, this);
                }
            } catch (Exception e) {
                logger.warn("failed to sample", e);
            }
        }
    }

    class SimpleNodeSampler extends NodeSampler {

        @Override
        protected void doSample() {
            HashSet<DiscoveryNode> newNodes = new HashSet<>();
            HashSet<DiscoveryNode> newFilteredNodes = new HashSet<>();
            for (DiscoveryNode listedNode : listedNodes) {
                if (!transportService.nodeConnected(listedNode)) {
                    try {
                        // its a listed node, light connect to it...
                        logger.trace("connecting to listed node (light) [{}]", listedNode);
                        transportService.connectToNodeLight(listedNode);
                    } catch (Throwable e) {
                        logger.debug("failed to connect to node [{}], removed from nodes list", e, listedNode);
                        continue;
                    }
                }
                try {
                    LivenessResponse livenessResponse = transportService.submitRequest(listedNode, TransportLivenessAction.NAME,
                            headers.applyTo(new LivenessRequest()),
                            TransportRequestOptions.options().withType(TransportRequestOptions.Type.STATE).withTimeout(pingTimeout),
                            new FutureTransportResponseHandler<LivenessResponse>() {
                                @Override
                                public LivenessResponse newInstance() {
                                    return new LivenessResponse();
                                }
                            }).txGet();
                    if (!ignoreClusterName && !clusterName.equals(livenessResponse.getClusterName())) {
                        logger.warn("node {} not part of the cluster {}, ignoring...", listedNode, clusterName);
                        newFilteredNodes.add(listedNode);
                    } else if (livenessResponse.getDiscoveryNode() != null) {
                        // use discovered information but do keep the original transport address, so people can control which address is exactly used.
                        DiscoveryNode nodeWithInfo = livenessResponse.getDiscoveryNode();
                        newNodes.add(new DiscoveryNode(nodeWithInfo.name(), nodeWithInfo.id(), nodeWithInfo.getHostName(), nodeWithInfo.getHostAddress(), listedNode.address(), nodeWithInfo.attributes(), nodeWithInfo.version()));
                    } else {
                        // although we asked for one node, our target may not have completed initialization yet and doesn't have cluster nodes
                        logger.debug("node {} didn't return any discovery info, temporarily using transport discovery node", listedNode);
                        newNodes.add(listedNode);
                    }
                } catch (Throwable e) {
                    logger.info("failed to get node info for {}, disconnecting...", e, listedNode);
                    transportService.disconnectFromNode(listedNode);
                }
            }

            nodes = validateNewNodes(newNodes);
            filteredNodes = Collections.unmodifiableList(new ArrayList<>(newFilteredNodes));
        }
    }

    class SniffNodesSampler extends NodeSampler {

        @Override
        protected void doSample() {
            // the nodes we are going to ping include the core listed nodes that were added
            // and the last round of discovered nodes
            Set<DiscoveryNode> nodesToPing = Sets.newHashSet();
            for (DiscoveryNode node : listedNodes) {
                nodesToPing.add(node);
            }
            for (DiscoveryNode node : nodes) {
                nodesToPing.add(node);
            }

            final CountDownLatch latch = new CountDownLatch(nodesToPing.size());
            final ConcurrentMap<DiscoveryNode, ClusterStateResponse> clusterStateResponses = ConcurrentCollections.newConcurrentMap();
            for (final DiscoveryNode listedNode : nodesToPing) {
                threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            if (!transportService.nodeConnected(listedNode)) {
                                try {

                                    // if its one of the actual nodes we will talk to, not to listed nodes, fully connect
                                    if (nodes.contains(listedNode)) {
                                        logger.trace("connecting to cluster node [{}]", listedNode);
                                        transportService.connectToNode(listedNode);
                                    } else {
                                        // its a listed node, light connect to it...
                                        logger.trace("connecting to listed node (light) [{}]", listedNode);
                                        transportService.connectToNodeLight(listedNode);
                                    }
                                } catch (Exception e) {
                                    logger.debug("failed to connect to node [{}], ignoring...", e, listedNode);
                                    latch.countDown();
                                    return;
                                }
                            }
                            //核心是在这里，刚刚开始初始化的时候，可能只有配置的一个节点，这个时候会通过这个地址发送一个state状态监测
                            //"cluster:monitor/state"
                            transportService.sendRequest(listedNode, ClusterStateAction.NAME,
                                    headers.applyTo(Requests.clusterStateRequest().clear().nodes(true).local(true)),
                                    TransportRequestOptions.options().withType(TransportRequestOptions.Type.STATE).withTimeout(pingTimeout),
                                    new BaseTransportResponseHandler<ClusterStateResponse>() {

                                        @Override
                                        public ClusterStateResponse newInstance() {
                                            return new ClusterStateResponse();
                                        }

                                        @Override
                                        public String executor() {
                                            return ThreadPool.Names.SAME;
                                        }

                                        @Override
                                        public void handleResponse(ClusterStateResponse response) {
                                            clusterStateResponses.put(listedNode, response);
                                            latch.countDown();
                                        }

                                        @Override
                                        public void handleException(TransportException e) {
                                            logger.info("failed to get local cluster state for {}, disconnecting...", e, listedNode);
                                            transportService.disconnectFromNode(listedNode);
                                            latch.countDown();
                                        }
                                    });
                        } catch (Throwable e) {
                            logger.info("failed to get local cluster state info for {}, disconnecting...", e, listedNode);
                            transportService.disconnectFromNode(listedNode);
                            latch.countDown();
                        }
                    }
                });
            }

            try {
                latch.await();
            } catch (InterruptedException e) {
                return;
            }

            HashSet<DiscoveryNode> newNodes = new HashSet<>();
            HashSet<DiscoveryNode> newFilteredNodes = new HashSet<>();
            for (Map.Entry<DiscoveryNode, ClusterStateResponse> entry : clusterStateResponses.entrySet()) {
                if (!ignoreClusterName && !clusterName.equals(entry.getValue().getClusterName())) {
                    logger.warn("node {} not part of the cluster {}, ignoring...", entry.getValue().getState().nodes().localNode(), clusterName);
                    newFilteredNodes.add(entry.getKey());
                    continue;
                }
                //接下来在这个地方拿到所有的data nodes 写入到nodes节点里边
                for (ObjectCursor<DiscoveryNode> cursor : entry.getValue().getState().nodes().dataNodes().values()) {
                    newNodes.add(cursor.value);
                }
            }

            nodes = validateNewNodes(newNodes);
            filteredNodes = Collections.unmodifiableList(new ArrayList<>(newFilteredNodes));
        }
    }

    public interface NodeListenerCallback<Response> {

        void doWithNode(DiscoveryNode node, ActionListener<Response> listener);
    }
}
