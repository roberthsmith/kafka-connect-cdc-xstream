package io.confluent.kafka.connect.cdc.xstream;

import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.CHAR;
import oracle.sql.DATE;
import oracle.sql.NUMBER;
import oracle.sql.RAW;
import oracle.sql.TIMESTAMP;
import oracle.streams.ChunkColumnValue;
import oracle.streams.ColumnValue;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;

import java.math.BigDecimal;
import java.sql.SQLException;

class ColumnValueConverter {


  public OracleColumnValue convertColumnValue(ColumnValue columnValue) throws SQLException {
    String columnName = columnValue.getColumnName();
    Schema schema = null;
    Object value = null;

    switch (columnValue.getColumnDataType()) {
      case ColumnValue.CHAR:
        CHAR charDatum = (CHAR) columnValue.getColumnData();
        schema = Schema.OPTIONAL_STRING_SCHEMA;
        if (null == charDatum || charDatum.isNull()) {
          value = null;
        } else {
          value = charDatum.stringValue();
        }
        break;
      case ColumnValue.NUMBER:
        NUMBER numberDatum = (NUMBER) columnValue.getColumnData();
        BigDecimal bigDecimal = numberDatum.bigDecimalValue();
        schema = Decimal.builder(bigDecimal.scale()).optional().build();
        value = bigDecimal;
        break;
      case ColumnValue.DATE:
        DATE dateDatum = (DATE) columnValue.getColumnData();
        schema = Date.builder().optional().build();
        value = dateDatum.dateValue();
        break;
      case ColumnValue.RAW:
        RAW rawDatum = (RAW) columnValue.getColumnData();
        schema = Schema.OPTIONAL_BYTES_SCHEMA;
        value = rawDatum.getBytes();
        break;
      case ColumnValue.TIMESTAMP:
        TIMESTAMP timestampDatum = (TIMESTAMP) columnValue.getColumnData();
        schema = Timestamp.builder().optional().build();
        value = timestampDatum.dateValue();
        break;
//        case ColumnValue.TIMESTAMPTZ:
//          break;
//        case ColumnValue.TIMESTAMPLTZ:
//          break;
      case ColumnValue.BINARY_FLOAT:
        BINARY_FLOAT binaryFloatDatum = (BINARY_FLOAT) columnValue.getColumnData();
        schema = Schema.OPTIONAL_FLOAT32_SCHEMA;
        value = binaryFloatDatum.floatValue();
        break;
      case ColumnValue.BINARY_DOUBLE:
        BINARY_DOUBLE binaryDoubleDatum = (BINARY_DOUBLE) columnValue.getColumnData();
        schema = Schema.OPTIONAL_FLOAT64_SCHEMA;
        value = binaryDoubleDatum.doubleValue();
        break;
//        case ColumnValue.INTERVALYM:
//          break;
//        case ColumnValue.INTERVALDS:
//          break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported ColumnValue %s(%s)", columnValue.getColumnName(), columnValue.getColumnDataType())
        );
    }


    return new OracleColumnValue(columnName, schema, value);
  }

  public OracleColumnValue convertChunkColumnValue(ChunkColumnValue columnValue) {
    String columnName = null;
    Schema schema = null;
    Object value = null;

    switch (columnValue.getChunkType()) {
      case ChunkColumnValue.CLOB:
      case ChunkColumnValue.NCLOB:
      case ChunkColumnValue.XMLTYPE:
        schema = Schema.OPTIONAL_STRING_SCHEMA;
        break;
      case ChunkColumnValue.BLOB:
      case ChunkColumnValue.LONG:
      case ChunkColumnValue.LONGRAW:
        schema = Schema.OPTIONAL_BYTES_SCHEMA;
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported ChunkColumnValue %s(%s)", columnValue.getColumnName(), columnValue.getChunkType())
        );
    }

    return new OracleColumnValue(columnName, schema, value);
  }

}
