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

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.cdc.xstream.type.TypeConversion;
import oracle.streams.ChunkColumnValue;
import oracle.streams.ColumnValue;
import oracle.streams.RowLCR;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;

class RecordConverter {
  private static final Logger log = LoggerFactory.getLogger(RecordConverter.class);
  final Callable<ChunkColumnValue> receiveChunkCallable;
  final SchemaGenerator schemaGenerator;
  final TypeConversion typeConversion;
  final XStreamSourceConnectorConfig config;
  final String xStreamServerName;

  RecordConverter(Callable<ChunkColumnValue> receiveChunkCallable, SchemaGenerator schemaGenerator, XStreamSourceConnectorConfig config, String xStreamServerName) {
    this.receiveChunkCallable = receiveChunkCallable;
    this.schemaGenerator = schemaGenerator;
    this.config = config;
    this.xStreamServerName = xStreamServerName;
    this.typeConversion = new TypeConversion();
  }

  public SourceRecord convert(RowLCR rowLCR) {
    SchemaPair schemaPair = null; //this.schemaGenerator.get(rowLCR);
    Struct valueStruct = new Struct(schemaPair.getValue());

//    Struct rowLCRMetadata = new Struct(SchemaGenerator.ROWLCR_METADATA);
//    rowLCRMetadata.put(SchemaGenerator.FIELD_SOURCE_DATABASE_NAME, rowLCR.getSourceDatabaseName());
//    rowLCRMetadata.put(SchemaGenerator.FIELD_SOURCE_OBJECT_NAME, rowLCR.getObjectName());
//    rowLCRMetadata.put(SchemaGenerator.FIELD_SOURCE_COMMAND_TYPE, rowLCR.getCommandType());
//    rowLCRMetadata.put(SchemaGenerator.FIELD_POSITION, rowLCR.getPosition());
//    rowLCRMetadata.put(SchemaGenerator.FIELD_SOURCE_OBJECT_OWNER, rowLCR.getObjectOwner());
//    Timestamp timestamp = rowLCR.getSourceTime().timestampValue();
//    rowLCRMetadata.put(SchemaGenerator.FIELD_SOURCE_TIME, new Date(timestamp.getTime()));
//    rowLCRMetadata.put(SchemaGenerator.FIELD_TAG, rowLCR.getTag());
//    rowLCRMetadata.put(SchemaGenerator.FIELD_TRANSACTION_ID, rowLCR.getTransactionId());
//    valueStruct.put(SchemaGenerator.ROWLCR_METADATA_FIELD, rowLCRMetadata);

    try {

      for (ColumnValue columnValue : rowLCR.getNewValues()) {
        if (log.isDebugEnabled()) {
          log.debug("Processing ColumnValue for column {}", columnValue.getColumnName());
        }

        Field field = schemaPair.getValue().field(columnValue.getColumnName());
        Object connectValue = this.typeConversion.toConnect(field.schema(), columnValue.getColumnData());
        if (log.isDebugEnabled()) {
          log.debug("Setting {} to '{}'.", field.name(), connectValue);
        }
        try {
          valueStruct.put(field, connectValue);
        } catch (DataException ex) {
          throw new DataException("Exception thrown while processing " + field.name(), ex);
        }
      }

      if (rowLCR.hasChunkData()) {
        ChunkColumnValue chunk;
        do {
          chunk = this.receiveChunkCallable.call();
          if (log.isDebugEnabled()) {
            log.debug("Processing ChunkColumnValue for column {}", chunk.getColumnName());
          }
          Field field = schemaPair.getValue().field(chunk.getColumnName());
          Object connectValue = this.typeConversion.toConnect(field.schema(), chunk.getColumnData());
          if (log.isDebugEnabled()) {
            log.debug("Setting {} to '{}'.", field.name(), connectValue);
          }
          try {
            valueStruct.put(field, connectValue);
          } catch (DataException ex) {
            throw new DataException("Exception thrown while processing " + field.name(), ex);
          }
        } while (!chunk.isEndOfRow());
      }
    } catch (Exception ex) {
      throw new ConnectException("Exception thrown while processing row.", ex);
    }

    Struct keyStruct = new Struct(schemaPair.getKey());
    for (Field field : schemaPair.getKey().fields()) {
      try {
        keyStruct.put(field, valueStruct.get(field.name()));
      } catch (DataException ex) {
        throw new DataException("Exception thrown while processing " + field.name(), ex);
      }
    }

    Map<String, ?> sourcePartition = ImmutableMap.of(
        "partiion", 1
    );
    Map<String, ?> sourceOffset = ImmutableMap.of(
        this.xStreamServerName,
        Position.toString(rowLCR.getPosition())
    );


//    String topic = this.config.getTemplateValue(rowLCR, this.config.topicNameTemplate);
//
//    SourceRecord sourceRecord = new SourceRecord(
//        sourcePartition,
//        sourceOffset,
//        topic,
//        schemaPair.getKey(),
//        keyStruct,
//        schemaPair.getValue(),
//        valueStruct
//    );

    return null;
  }

}
