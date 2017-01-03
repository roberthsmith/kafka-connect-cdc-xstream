package io.confluent.kafka.connect.cdc.xstream;

import oracle.streams.ChunkColumnValue;
import oracle.streams.LCR;
import oracle.streams.StreamsException;
import oracle.streams.XStreamOut;

import java.sql.Connection;

class XStreamOutputImpl implements XStreamOutput {
  final XStreamOut xStreamOut;
  final Connection connection;

  XStreamOutputImpl(XStreamOut xStreamOut, Connection connection) {
    this.xStreamOut = xStreamOut;
    this.connection = connection;
  }

  @Override
  public LCR receiveLCR() throws StreamsException {
    return this.xStreamOut.receiveLCR(XStreamOut.DEFAULT_MODE | XStreamOut.NEW_COLUMN_ONLY_MODE);
  }

  @Override
  public ChunkColumnValue receiveChunk() throws StreamsException {
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
    this.xStreamOut.detach(XStreamOut.DEFAULT_MODE);
  }

  @Override
  public Connection connection() {
    return this.connection;
  }
}
