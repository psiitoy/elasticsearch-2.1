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

package org.elasticsearch.action;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.*;

/**
 * A generic proxy that will execute the given action against a specific node.
 */
public class TransportActionNodeProxy<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent {

    private final TransportService transportService;
    private final GenericAction<Request, Response> action;
    private final TransportRequestOptions transportOptions;

    @Inject
    public TransportActionNodeProxy(Settings settings, GenericAction<Request, Response> action, TransportService transportService) {
        // 日志初始化(从setting中获取日志配置相关信息) 赋值setting
        super(settings);
        // 请求名字
        this.action = action;
        // 传输器
        this.transportService = transportService;
        // 请求类型+是否压缩+超时时间 配置
        this.transportOptions = action.transportOptions(settings);
    }

    //代理的行为
    public void execute(final DiscoveryNode node, final Request request, final ActionListener<Response> listener) {
        /**
         * 请求校验(匿名内部类)
         * @see GetRequest#validate(),SearchRequest#validate()
         *
         */
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        // 通过netty的channel将数据写入
        transportService.sendRequest(node, action.name(), request, transportOptions, new ActionListenerResponseHandler<Response>(listener) {
            @Override
            public Response newInstance() {
                return action.newResponse();
            }
        });
    }
}
