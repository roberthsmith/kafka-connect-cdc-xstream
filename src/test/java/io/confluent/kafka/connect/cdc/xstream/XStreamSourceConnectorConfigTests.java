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

import io.confluent.kafka.connect.utils.config.MarkdownFormatter;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class XStreamSourceConnectorConfigTests {
  public static Map<String, String> settings() {
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put(XStreamSourceConnectorConfig.JDBC_URL_CONF, "jdbc:oracle:thin:scott/tiger@myhost:1521:orcl");
    settings.put(XStreamSourceConnectorConfig.JDBC_PASSWORD_CONF, "tiger");
    settings.put(XStreamSourceConnectorConfig.JDBC_USERNAME_CONF, "scott");
    settings.put(XStreamSourceConnectorConfig.XSTREAM_SERVER_NAMES_CONF, "xstreamout");
    return settings;
  }

  @Test
  public void doc() {
    System.out.println(
        MarkdownFormatter.toMarkdown(XStreamSourceConnectorConfig.config())
    );
  }
}
