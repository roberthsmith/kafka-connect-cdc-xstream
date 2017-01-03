package io.confluent.kafka.connect.cdc.xstream;

import oracle.streams.ChunkColumnValue;
import oracle.streams.LCR;
import oracle.streams.StreamsException;

import java.sql.Connection;

public interface XStreamOutput {
  LCR receiveLCR() throws StreamsException;

  ChunkColumnValue receiveChunk() throws StreamsException;

  int getBatchStatus() throws StreamsException;

  byte[] getFetchLowWatermark() throws StreamsException;

  void setProcessedLowWatermark(byte[] var1) throws StreamsException;

  void setProcessedLowWatermark(byte[] var1, byte[] var2) throws StreamsException;

  void detach() throws StreamsException;

  Connection connection();
}
