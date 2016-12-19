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

import org.junit.Before;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class XStreamSourceConnectorTest {
  public XStreamSourceConnector xStreamSourceConnector;
  Map<String, String> settings;

  @Before
  public void setup() {
    this.xStreamSourceConnector = new XStreamSourceConnector();
    this.settings = XStreamSourceConnectorConfigTest.settings();
  }

  @Test
  public void start() {
    this.xStreamSourceConnector.start(this.settings);
  }

  @Test
  public void stop() {
    this.xStreamSourceConnector.start(this.settings);
    this.xStreamSourceConnector.stop();
  }

  @Test
  public void taskClass() {
    assertEquals(XStreamSourceTask.class, this.xStreamSourceConnector.taskClass());
  }

  @Test
  public void taskConfigs() {
    this.xStreamSourceConnector.start(this.settings);
    List<Map<String, String>> taskConfigs = this.xStreamSourceConnector.taskConfigs(10);
    assertNotNull(taskConfigs);
    assertEquals(1, taskConfigs.size());
    assertEquals(this.settings, taskConfigs.get(0));
  }

  @Test
  public void version() {
    assertNotNull(this.xStreamSourceConnector.version());
  }

}
