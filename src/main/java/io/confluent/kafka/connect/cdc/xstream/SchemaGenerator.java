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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import freemarker.template.TemplateException;
import io.confluent.kafka.connect.cdc.xstream.schema.Column;
import io.confluent.kafka.connect.cdc.xstream.schema.TableMetadataProvider;
import oracle.streams.ChunkColumnValue;
import oracle.streams.ColumnValue;
import oracle.streams.LCR;
import oracle.streams.RowLCR;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

class SchemaGenerator {
  public static final String FIELD_SOURCE_DATABASE_NAME = "SourceDatabaseName";
  public static final String FIELD_SOURCE_COMMAND_TYPE = "CommandType";
  public static final String FIELD_SOURCE_OBJECT_OWNER = "ObjectOwner";
  public static final String FIELD_SOURCE_OBJECT_NAME = "ObjectName";
  public static final String FIELD_TAG = "Tag";
  public static final String FIELD_POSITION = "Position";
  public static final String FIELD_TRANSACTION_ID = "TransactionId";
  public static final String FIELD_SOURCE_TIME = "SourceTime";
  private static final Logger log = LoggerFactory.getLogger(SchemaGenerator.class);

  final Connection connection;
  final TableMetadataProvider tableMetadataProvider;
  final XStreamSourceConnectorConfig config;

  public SchemaGenerator(XStreamSourceConnectorConfig config, Connection connection, TableMetadataProvider tableMetadataProvider) throws SQLException {
    this.connection = connection;
    this.config = config;
    this.tableMetadataProvider = tableMetadataProvider;
  }


  Schema createColumnSchema(Column column) {
    SchemaBuilder schemaBuilder;

    if (column.chunkColumn()) {
      switch (column.type()) {
        case ChunkColumnValue.CLOB:
        case ChunkColumnValue.NCLOB:
        case ChunkColumnValue.XMLTYPE:
          schemaBuilder = SchemaBuilder.string();
          break;
        case ChunkColumnValue.BLOB:
        case ChunkColumnValue.LONG:
        case ChunkColumnValue.LONGRAW:
          schemaBuilder = SchemaBuilder.bytes();
          break;
        default:
          throw new UnsupportedOperationException("Unsupported ChunkColumnValue " + column.type());
      }
    } else {
      switch (column.type()) {
        case ColumnValue.CHAR:
          schemaBuilder = SchemaBuilder.string();
          break;
        case ColumnValue.NUMBER:
          int scale = column.scale() == null ? 0 : column.scale();
          schemaBuilder = Decimal.builder(scale);
          break;
        case ColumnValue.DATE:
          schemaBuilder = Date.builder();
          break;
        case ColumnValue.RAW:
          schemaBuilder = SchemaBuilder.bytes();
          break;
        case ColumnValue.TIMESTAMP:
          schemaBuilder = Timestamp.builder();
          break;
//        case ColumnValue.TIMESTAMPTZ:
//          break;
//        case ColumnValue.TIMESTAMPLTZ:
//          break;
        case ColumnValue.BINARY_FLOAT:
          schemaBuilder = SchemaBuilder.float32();
          break;
        case ColumnValue.BINARY_DOUBLE:
          schemaBuilder = SchemaBuilder.float64();
          break;
//        case ColumnValue.INTERVALYM:
//          break;
//        case ColumnValue.INTERVALDS:
//          break;
        default:
          throw new UnsupportedOperationException("Unsupported ChunkColumnValue " + column.type());
      }
    }


    if (null != column.comments() && !column.comments().isEmpty()) {
      schemaBuilder.doc(column.comments());
    }

    if (column.nullable()) {
      schemaBuilder.optional();
    }

    schemaBuilder.parameter("xstream.chunk", String.valueOf(column.chunkColumn()));
    schemaBuilder.parameter("xstream.type", String.valueOf(column.type()));

    return schemaBuilder.build();
  }

//  SchemaPair generate(RowLCR row) throws SQLException, IOException, TemplateException {
//    String keySchemaName = this.config.getTemplateValue(row, this.config.keyNameTemplate);
//    String valueSchemaName = this.config.getTemplateValue(row, this.config.valueNameTemplate);
//
//    SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct();
//
//    valueSchemaBuilder.field(ROWLCR_METADATA_FIELD, ROWLCR_METADATA);
//
//    Map<String, Schema> columnSchemas = new HashMap<>();
//    List<Column> columns = this.tableMetadataProvider.getColumns(row);
//    List<String> keys = this.tableMetadataProvider.getKeys(row);
//
//    for (Column column : columns) {
//      if (log.isDebugEnabled()) {
//        log.debug("Processing Column: {}", column);
//      }
//
//      Schema columnSchema = createColumnSchema(column);
//
//      if (log.isDebugEnabled()) {
//        log.debug("Mapped {} to {}", column, columnSchema.type());
//      }
//
//      columnSchemas.put(column.name(), columnSchema);
//      valueSchemaBuilder.field(column.name(), columnSchema);
//    }
//
//    valueSchemaBuilder.name(valueSchemaName);
//    Schema valueSchema = valueSchemaBuilder.build();
//    SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
//
//    for (String key : keys) {
//      Schema columnSchema = columnSchemas.get(key);
//
//      if (null != columnSchema) {
//        if (log.isDebugEnabled()) {
//          log.debug("Found key {}", key);
//        }
//        keySchemaBuilder.field(key, columnSchema);
//      }
//
//
//    }
//
//    keySchemaBuilder.name(keySchemaName);
//    Schema keySchema = keySchemaBuilder.build();
//    return new SchemaPair(keySchema, valueSchema);
//  }
//
//  public void invalidate(final LCR row) {
//    final LCRKey lcrKey = new LCRKey(row);
//    this.schemaCache.invalidate(lcrKey);
//  }
//
//  public SchemaPair get(final RowLCR row) {
//    final LCRKey lcrKey = new LCRKey(row);
//
//    if (log.isDebugEnabled()) {
//      log.debug("{}", lcrKey);
//    }
//
//    try {
//      return this.schemaCache.get(lcrKey, new Callable<SchemaPair>() {
//        @Override
//        public SchemaPair call() throws Exception {
//          return generate(row);
//        }
//      });
//    } catch (ExecutionException e) {
//      throw new ConnectException(e);
//    }
//  }
}
