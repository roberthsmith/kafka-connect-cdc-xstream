package io.confluent.kafka.connect.cdc.xstream.schema;

public interface Column {
  /**
   * Name of the column
   *
   * @return
   */
  String name();

  /**
   * Data type for the column.
   *
   * @return
   * @see ColumnValue
   */
  int type();

  /**
   * Comments for the table.
   *
   * @return
   */
  String comments();

  /**
   * Scale for the data type.
   *
   * @return
   */
  Integer scale();

  /**
   * Precision for the table.
   *
   * @return
   */
  Integer precision();

  /**
   * Flag to determine if the table is nullable.
   *
   * @return
   */
  boolean nullable();

  /**
   * Flag to determine if the column is a chunkColumnValue
   *
   * @return
   */
  boolean chunkColumn();
}
