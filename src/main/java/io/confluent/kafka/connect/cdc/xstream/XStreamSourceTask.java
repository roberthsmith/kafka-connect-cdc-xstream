/**
 * Copyright (C) 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.connect.cdc.xstream;

import com.google.common.util.concurrent.ServiceManager;
import io.confluent.kafka.connect.cdc.CDCSourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class XStreamSourceTask extends CDCSourceTask<XStreamSourceConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(XStreamSourceTask.class);
  ServiceManager serviceManager;

  @Override
  protected XStreamSourceConnectorConfig getConfig(Map<String, String> map) {
    return new XStreamSourceConnectorConfig(map);
  }

  @Override
  public void start(Map<String, String> map) {
    super.start(map);
    QueryService queryService = new QueryService(this.config, this.context.offsetStorageReader(), this);
    this.serviceManager = new ServiceManager(Arrays.asList(queryService));
    this.serviceManager.startAsync();
  }

  @Override
  public void stop() {
    if (log.isInfoEnabled()) {
      log.info("Queries in flight can take a while to finish. No more queries will be issued.");
    }
    this.serviceManager.stopAsync();

    try {
      this.serviceManager.awaitStopped(5, TimeUnit.MINUTES);
    } catch (TimeoutException e) {
      if (log.isErrorEnabled()) {
        log.error("Timeout exceeded waiting for the service to stop.");
      }
    }
  }
}
