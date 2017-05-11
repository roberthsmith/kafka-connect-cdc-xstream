/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc.xstream;

import com.google.common.base.Stopwatch;
import oracle.jdbc.OracleConnection;
import oracle.streams.ChunkColumnValue;
import oracle.streams.LCR;
import oracle.streams.StreamsException;
import oracle.streams.XStreamOut;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;

class XStreamOutputImpl implements XStreamOutput {
  final static Logger log = LoggerFactory.getLogger(XStreamOutputImpl.class);
  final XStreamOut xStreamOut;
  final Connection connection;
  final Time time = new SystemTime();

  private XStreamOutputImpl(XStreamOut xStreamOut, Connection connection) {
    this.xStreamOut = xStreamOut;
    this.connection = connection;
  }

  public static XStreamOutput attach(OracleConnection connection, OracleSourceConnectorConfig config, byte[] position) throws StreamsException {
    String server = config.xStreamServerNames.get(0);
    log.info("Attaching to xStream output '{}'", server);
    XStreamOut xStreamOut = XStreamOut.attach(connection, server, position, config.xStreamBatchInterval, config.xStreamIdleTimeout, XStreamOut.DEFAULT_MODE);
    return new XStreamOutputImpl(xStreamOut, connection);
  }

  @Override
  public LCR receiveLCR() throws StreamsException {
    log.trace("receiveLCR() - XStreamOut.receiveLCR(XStreamOut.DEFAULT_MODE | XStreamOut.NEW_COLUMN_ONLY_MODE);");
    return this.xStreamOut.receiveLCR(XStreamOut.DEFAULT_MODE | XStreamOut.NEW_COLUMN_ONLY_MODE);
  }

  @Override
  public ChunkColumnValue receiveChunk() throws StreamsException {
    log.trace("receiveChunk() - XStreamOut.receiveChunk(XStreamOut.DEFAULT_MODE);");
    return this.xStreamOut.receiveChunk(XStreamOut.DEFAULT_MODE);
  }

  @Override
  public int getBatchStatus() throws StreamsException {
    return this.xStreamOut.getBatchStatus();
  }

  @Override
  public byte[] getFetchLowWatermark() throws StreamsException {
    return this.xStreamOut.getFetchLowWatermark();
  }

  @Override
  public void setProcessedLowWatermark(byte[] var1) throws StreamsException {
    this.xStreamOut.setProcessedLowWatermark(var1, XStreamOut.DEFAULT_MODE);
  }

  @Override
  public void setProcessedLowWatermark(byte[] var1, byte[] var2) throws StreamsException {
    this.xStreamOut.setProcessedLowWatermark(var1, var2, XStreamOut.DEFAULT_MODE);
  }

  @Override
  public void detach() throws StreamsException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    while (XStreamOut.EXECUTING == getBatchStatus() && stopwatch.elapsed(TimeUnit.SECONDS) < 30) {
      log.trace("detach() - Waiting for current batch to finish.");
      time.sleep(500);
    }
    log.trace("detach() - XStreamOut.detach().");
    this.xStreamOut.detach(XStreamOut.DEFAULT_MODE);
    log.trace("detach() - XStreamOut.detach() complete.");
  }
}
