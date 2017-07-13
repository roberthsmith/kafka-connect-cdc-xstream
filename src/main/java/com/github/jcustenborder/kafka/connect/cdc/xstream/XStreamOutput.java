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

import oracle.streams.ChunkColumnValue;
import oracle.streams.LCR;
import oracle.streams.StreamsException;

public interface XStreamOutput {
  LCR receiveLCR() throws StreamsException;

  ChunkColumnValue receiveChunk() throws StreamsException;

  int getBatchStatus() throws StreamsException;

  byte[] getFetchLowWatermark() throws StreamsException;

  void setProcessedLowWatermark(byte[] var1) throws StreamsException;

  void setProcessedLowWatermark(byte[] var1, byte[] var2) throws StreamsException;

  void detach() throws StreamsException;
}
