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

import com.google.common.collect.ImmutableSet;
import com.github.jcustenborder.kafka.connect.cdc.PooledCDCSourceConnectorConfig;
import oracle.streams.RowLCR;
import oracle.streams.XStreamOut;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

class OracleSourceConnectorConfig extends PooledCDCSourceConnectorConfig<OracleConnectionPoolDataSourceFactory> {


  public static final String XSTREAM_SERVER_NAMES_CONF = "oracle.xstream.server.names";
  public static final String XSTREAM_BATCH_INTERVAL_CONF = "oracle.xstream.batch.interval";
  public static final String XSTREAM_IDLE_TIMEOUT_CONF = "oracle.xstream.idle.timeout";
  public static final String XSTREAM_RECEIVE_WAIT_CONF = "oracle.xstream.receive.wait.ms";

  public static final String XSTREAM_ALLOWED_COMMANDS_CONF = "xstream.allowed.commands";
  static final String XSTREAM_SERVER_NAMES_DOC = "Name of the XStream outbound servers.";
  static final String XSTREAM_BATCH_INTERVAL_DOC = "XStreamOut batch processing interval.";
  static final String XSTREAM_IDLE_TIMEOUT_DOC = "XStreamOut idle timeout value.";
  static final String XSTREAM_RECEIVE_WAIT_DOC = "The amount of time to wait in milliseconds when XStreamOut.receiveChange() returns null";
  static final String XSTREAM_ALLOWED_COMMANDS_DOC = "The commands the task should process.";
  static final int XSTREAM_RECEIVE_WAIT_DEFAULT = 1000;
  static final List<String> XSTREAM_ALLOWED_COMMANDS_DEFAULT = Arrays.asList(RowLCR.INSERT, RowLCR.UPDATE);
  public final List<String> xStreamServerNames;
  public final int xStreamBatchInterval;
  public final int xStreamIdleTimeout;
  public final int xStreamReceiveWait;
  public final Set<String> allowedCommands;

  public OracleSourceConnectorConfig(Map<?, ?> parsedConfig) {
    super(config(), parsedConfig);
    this.xStreamReceiveWait = this.getInt(XSTREAM_RECEIVE_WAIT_CONF);
    this.allowedCommands = ImmutableSet.copyOf(this.getList(XSTREAM_ALLOWED_COMMANDS_CONF));
    this.xStreamServerNames = this.getList(XSTREAM_SERVER_NAMES_CONF);
    this.xStreamBatchInterval = this.getInt(XSTREAM_BATCH_INTERVAL_CONF);
    this.xStreamIdleTimeout = this.getInt(XSTREAM_IDLE_TIMEOUT_CONF);
  }

  public static ConfigDef config() {
    return PooledCDCSourceConnectorConfig.config()
        .define(XSTREAM_RECEIVE_WAIT_CONF, ConfigDef.Type.INT, XSTREAM_RECEIVE_WAIT_DEFAULT, ConfigDef.Importance.LOW, XSTREAM_RECEIVE_WAIT_DOC)
        .define(XSTREAM_ALLOWED_COMMANDS_CONF, ConfigDef.Type.LIST, XSTREAM_ALLOWED_COMMANDS_DEFAULT, ConfigDef.Importance.LOW, XSTREAM_ALLOWED_COMMANDS_DOC)
        .define(XSTREAM_SERVER_NAMES_CONF, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, XSTREAM_SERVER_NAMES_DOC)
        .define(XSTREAM_BATCH_INTERVAL_CONF, ConfigDef.Type.INT, XStreamOut.DEFAULT_BATCH_INTERVAL, ConfigDef.Importance.MEDIUM, XSTREAM_BATCH_INTERVAL_DOC)
        .define(XSTREAM_IDLE_TIMEOUT_CONF, ConfigDef.Type.INT, XStreamOut.DEFAULT_IDLE_TIMEOUT, ConfigDef.Importance.MEDIUM, XSTREAM_IDLE_TIMEOUT_DOC);
  }

  @Override
  public OracleConnectionPoolDataSourceFactory connectionPoolDataSourceFactory() {
    return new OracleConnectionPoolDataSourceFactory(this);
  }
}
