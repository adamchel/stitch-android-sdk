/*
 * Copyright 2018-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.stitch.server.services.http;

import com.mongodb.stitch.core.services.http.HttpRequest;
import com.mongodb.stitch.core.services.http.HttpResponse;
import com.mongodb.stitch.core.services.http.internal.CoreHttpServiceClient;
import com.mongodb.stitch.server.core.services.internal.NamedServiceClientFactory;
import com.mongodb.stitch.server.services.http.internal.HttpServiceClientImpl;

import javax.annotation.Nonnull;

/**
 * The HTTP service client.
 */
public interface HttpServiceClient {

  /**
   * Executes the given {@link HttpRequest}.
   *
   * @param request the request to execute.
   * @return the response to executing the request.
   */
  HttpResponse execute(@Nonnull final HttpRequest request);

  NamedServiceClientFactory<HttpServiceClient> factory =
      (service, appInfo) -> new HttpServiceClientImpl(new CoreHttpServiceClient(service));
}
