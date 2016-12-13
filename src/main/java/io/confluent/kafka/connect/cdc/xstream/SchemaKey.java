package io.confluent.kafka.connect.cdc.xstream;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;

class SchemaKey implements Comparable<SchemaKey> {
  final String schemaName;
  final String tableName;

  SchemaKey(String schemaName, String tableName) {
    this.schemaName = schemaName;
    this.tableName = tableName;
  }


  @Override
  public int compareTo(SchemaKey that) {
    return ComparisonChain.start()
        .compare(this.schemaName, that.schemaName)
        .compare(this.tableName, that.tableName)
        .result();
  }

  @Override
  public boolean equals(Object obj) {
    if(obj instanceof SchemaKey){
      return 0==compareTo((SchemaKey)obj);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(SchemaKey.class)
        .add("schemaName", this.schemaName)
        .add("tableName", this.tableName)
        .omitNullValues()
        .toString();
  }
}
