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
package com.jd.myplugin;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.RestRequest.Method;

/**
 * Created by wangdi on 17-6-20.
 */
public class MyService extends BaseRestHandler {
    //注入对象
    @Inject
    protected MyService(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        //将该Handler绑定到某访问路径
        controller.registerHandler(Method.GET, "/hello/", this);
        controller.registerHandler(Method.GET, "/hello/{name}", this);
    }

    //处理绑定路径的请求访问
    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        logger.debug("HelloWorldAction.handleRequest called");
        final String name = request.hasParam("name") ? request.param("name") : "world";

        String content = "{\"success\":true, \"message\":\"hello " + name + "\"}";

        RestResponse response = new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, content);
        channel.sendResponse(response);
    }
}
