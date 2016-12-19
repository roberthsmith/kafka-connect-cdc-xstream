package io.confluent.kafka.connect.cdc.xstream;

import io.confluent.kafka.connect.cdc.Change;
import org.apache.kafka.connect.data.Schema;

class OracleColumnValue implements Change.ColumnValue {
  final String columnName;
  final Schema schema;
  final Object value;

  OracleColumnValue(String columnName, Schema schema, Object value) {
    this.columnName = columnName;
    this.schema = schema;
    this.value = value;
  }

  @Override
  public String columnName() {
    return this.columnName;
  }

  @Override
  public Schema schema() {
    return this.schema;
  }

  @Override
  public Object value() {
    return this.value;
  }
}
