package io.confluent.kafka.connect.cdc.xstream;

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
    if (log.isInfoEnabled()) {
      log.info("Attaching to xStream output '{}'", server);
    }
    XStreamOut xStreamOut = XStreamOut.attach(connection, server, position, config.xStreamBatchInterval, config.xStreamIdleTimeout, XStreamOut.DEFAULT_MODE);
    return new XStreamOutputImpl(xStreamOut, connection);
  }

  @Override
  public LCR receiveLCR() throws StreamsException {
    if (log.isTraceEnabled()) {
      log.trace("XStreamOut.receiveLCR(XStreamOut.DEFAULT_MODE | XStreamOut.NEW_COLUMN_ONLY_MODE);");
    }

    return this.xStreamOut.receiveLCR(XStreamOut.DEFAULT_MODE | XStreamOut.NEW_COLUMN_ONLY_MODE);
  }

  @Override
  public ChunkColumnValue receiveChunk() throws StreamsException {
    if (log.isTraceEnabled()) {
      log.trace("XStreamOut.receiveChunk(XStreamOut.DEFAULT_MODE);");
    }
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
      if (log.isTraceEnabled()) {
        log.trace("Waiting for current batch to finish.");
      }
      time.sleep(500);
    }
    if (log.isTraceEnabled()) {
      log.trace("XStreamOut.detach().");
    }
    this.xStreamOut.detach(XStreamOut.DEFAULT_MODE);
    if (log.isTraceEnabled()) {
      log.trace("XStreamOut.detach() complete.");
    }
  }
}
