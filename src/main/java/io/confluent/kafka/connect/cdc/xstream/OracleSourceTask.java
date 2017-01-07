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

import com.google.common.util.concurrent.Service;
import io.confluent.kafka.connect.cdc.BaseServiceTask;
import io.confluent.kafka.connect.cdc.ChangeWriter;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Map;

class OracleSourceTask extends BaseServiceTask<OracleSourceConnectorConfig> {
  ChangeWriter changeWriter;

  @Override
  protected Service service(ChangeWriter changeWriter, OffsetStorageReader offsetStorageReader) {
    this.changeWriter = changeWriter;
    return new QueryService(this.config, offsetStorageReader, this.changeWriter);
  }

  @Override
  protected OracleSourceConnectorConfig getConfig(Map<String, String> map) {
    return new OracleSourceConnectorConfig(map);
  }
}