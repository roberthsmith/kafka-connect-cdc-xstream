package io.confluent.kafka.connect.cdc.xstream;

import io.confluent.kafka.connect.cdc.Change;
import oracle.streams.RowLCR;

import java.util.List;
import java.util.Map;

class OracleChange implements Change {
  final RowLCR rowLCR;

  OracleChange(RowLCR rowLCR) {
    this.rowLCR = rowLCR;
  }

  @Override
  public Map<String, String> metadata() {
    return null;
  }

  @Override
  public Map<String, Object> sourcePartition() {
    return null;
  }

  @Override
  public Map<String, Object> sourceOffset() {
    return null;
  }

  @Override
  public String schemaName() {
    return this.rowLCR.getSourceDatabaseName();
  }

  @Override
  public String tableName() {
    return this.rowLCR.getObjectName();
  }

  @Override
  public List<ColumnValue> keyColumns() {
    return null;
  }

  @Override
  public List<ColumnValue> valueColumns() {
    return null;
  }

  @Override
  public ChangeType changeType() {
    ChangeType result;

    switch (this.rowLCR.getCommandType()) {
      case RowLCR.INSERT:
        result = ChangeType.INSERT;
        break;
      case RowLCR.UPDATE:
        result = ChangeType.UPDATE;
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("CommandType of '%s' is not supported.", this.rowLCR.getCommandType())
        );
    }

    return result;
  }

  @Override
  public long timestamp() {
    return this.rowLCR.getSourceTime().timestampValue().getTime();
  }

}
