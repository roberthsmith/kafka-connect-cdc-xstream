/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc.xstream;

import org.junit.jupiter.api.Disabled;

@Disabled
public class XStreamSourceTask11GTest extends Oracle11gTest {
//  OracleSourceTask xStreamSourceTask;
//
//  @BeforeEach
//  public void before(
//      @DockerFormatString(container = XStreamTestConstants.ORACLE_CONTAINER, port = XStreamTestConstants.ORACLE_PORT, format = XStreamTestConstants.JDBC_URL_FORMAT_11G) String jdbcUrl
//  ) {
//    Map<String, String> settings = ImmutableMap.of(
//        OracleSourceConnectorConfig.JDBC_URL_CONF, jdbcUrl,
//        OracleSourceConnectorConfig.JDBC_USERNAME_CONF, XStreamTestConstants.XSTREAM_USERNAME_12C,
//        OracleSourceConnectorConfig.JDBC_PASSWORD_CONF, XStreamTestConstants.XSTREAM_PASSWORD_12C,
//        OracleSourceConnectorConfig.XSTREAM_SERVER_NAMES_CONF, "xout"
//    );
//
//    this.xStreamSourceTask = new OracleSourceTask();
//
//    SourceTaskContext context = mock(SourceTaskContext.class);
//    OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
//    when(offsetStorageReader.offset(anyMap())).thenReturn(new HashMap());
//    when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
//    this.xStreamSourceTask.initialize(context);
//    this.xStreamSourceTask.start(settings);
//  }
//
//  @Test
//  public void foo() {
//
//  }
//
//  @AfterEach
//  public void stop() {
//    this.xStreamSourceTask.stop();
//  }
//

}
