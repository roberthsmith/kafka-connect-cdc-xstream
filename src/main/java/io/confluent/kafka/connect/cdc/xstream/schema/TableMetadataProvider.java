package io.confluent.kafka.connect.cdc.xstream.schema;

import com.google.common.base.Charsets;
import com.google.common.base.MoreObjects;
import com.google.common.io.Resources;
import oracle.streams.ChunkColumnValue;
import oracle.streams.ColumnValue;
import oracle.streams.RowLCR;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;

public abstract class TableMetadataProvider {
  static final Pattern PATTERN_INTERVALDS = Pattern.compile("^INTERVAL DAY\\(\\d+\\) TO SECOND\\(\\d+\\)$", Pattern.CASE_INSENSITIVE);
  static final Pattern PATTERN_TIMESTAMP = Pattern.compile("^TIMESTAMP\\(\\d+\\)$", Pattern.CASE_INSENSITIVE);
  static final Pattern PATTERN_TIMESTAMP_WITH_TIME_ZONE = Pattern.compile("^TIMESTAMP\\(\\d+\\)\\s+WITH\\s+TIME\\s+ZONE$", Pattern.CASE_INSENSITIVE);
  static final Pattern PATTERN_TIMESTAMP_WITH_LOCAL_TIME_ZONE = Pattern.compile("^TIMESTAMP\\(\\d+\\)\\s+WITH\\s+LOCAL\\s+TIME\\s+ZONE$", Pattern.CASE_INSENSITIVE);
  static final Pattern PATTERN_LONG_RAW = Pattern.compile("^LONG\\s+RAW$", Pattern.CASE_INSENSITIVE);
  private static final Logger log = LoggerFactory.getLogger(TableMetadataProvider.class);
  protected final Connection connection;

  protected TableMetadataProvider(Connection connection) {
    this.connection = connection;
  }

  public static TableMetadataProvider get(Connection connection) throws SQLException {
    DatabaseMetaData databaseMetaData = connection.getMetaData();

    TableMetadataProvider tableMetadataProvider;

    switch (databaseMetaData.getDatabaseMajorVersion()) {
      case 12:
        tableMetadataProvider = new Oracle12CTableMetadataProvider(connection);
        break;
      //TODO: Add detection for 11G
      default:
        throw new UnsupportedOperationException(
            String.format("Database version %s is not supported.", databaseMetaData.getDatabaseMajorVersion())
        );
    }

    return tableMetadataProvider;
  }

  public abstract List<Column> getColumns(RowLCR rowLCR) throws SQLException;

  public abstract List<String> getKeys(RowLCR rowLCR) throws SQLException;

  protected String loadSQLResource(String fileName) {
    URL resourceUrl = Resources.getResource(this.getClass(), fileName);
    try {
      return Resources.toString(resourceUrl, Charsets.UTF_8);
    } catch (IOException ex) {
      throw new IllegalStateException("Could not load sql resource '" + fileName + "'", ex);
    }
  }

  protected PreparedStatement prepareSQLResource(String fileName) throws SQLException {
    String sql = loadSQLResource(fileName);
    if (log.isDebugEnabled()) {
      log.debug("prepareStatement(\"{}\")", sql);
    }
    return this.connection.prepareStatement(sql);
  }

  Column column(ResultSet resultSet) throws SQLException {
    DefaultColumn column = new DefaultColumn();
    column.name = resultSet.getString("COLUMN_NAME");
    column.comments = resultSet.getString("COMMENTS");
    String nullable = resultSet.getString("NULLABLE");
    column.nullable = "Y".equalsIgnoreCase(nullable);

    int precision = resultSet.getInt("DATA_PRECISION");
    if (!resultSet.wasNull()) {
      column.precision = precision;
    }
    int scale = resultSet.getInt("DATA_SCALE");
    if (!resultSet.wasNull()) {
      column.scale = scale;
    }

    String dataType = resultSet.getString("DATA_TYPE");

    if ("BINARY_DOUBLE".equalsIgnoreCase(dataType)) {
      column.type = ColumnValue.BINARY_DOUBLE;
      column.chunkColumn = false;
    } else if ("BINARY_FLOAT".equalsIgnoreCase(dataType)) {
      column.type = ColumnValue.BINARY_FLOAT;
      column.chunkColumn = false;
    } else if ("NUMBER".equalsIgnoreCase(dataType)) {
      column.type = ColumnValue.NUMBER;
      column.chunkColumn = false;
    } else if ("CHAR".equalsIgnoreCase(dataType)) {
      column.type = ColumnValue.CHAR;
      column.chunkColumn = false;
    } else if ("DATE".equalsIgnoreCase(dataType)) {
      column.type = ColumnValue.DATE;
      column.chunkColumn = false;
    } else if ("RAW".equalsIgnoreCase(dataType)) {
      column.type = ColumnValue.RAW;
      column.chunkColumn = false;
    } else if ("CLOB".equalsIgnoreCase(dataType)) {
      column.type = ChunkColumnValue.CLOB;
      column.chunkColumn = true;
    } else if ("BLOB".equalsIgnoreCase(dataType)) {
      column.type = ChunkColumnValue.BLOB;
      column.chunkColumn = true;
    } else if (PATTERN_INTERVALDS.matcher(dataType).matches()) {
      column.type = ColumnValue.INTERVALDS;
      column.chunkColumn = false;
    } else if (PATTERN_TIMESTAMP.matcher(dataType).matches()) {
      column.type = ColumnValue.TIMESTAMP;
      column.chunkColumn = false;
    } else if (PATTERN_TIMESTAMP_WITH_TIME_ZONE.matcher(dataType).matches()) {
      column.type = ColumnValue.TIMESTAMPTZ;
      column.chunkColumn = false;
    } else if (PATTERN_TIMESTAMP_WITH_LOCAL_TIME_ZONE.matcher(dataType).matches()) {
      column.type = ColumnValue.TIMESTAMPLTZ;
      column.chunkColumn = false;
    } else if ("NCLOB".equalsIgnoreCase(dataType)) {
      column.type = ChunkColumnValue.NCLOB;
      column.chunkColumn = true;
    } else if ("NCHAR".equalsIgnoreCase(dataType)) {
      column.type = ChunkColumnValue.CHAR;
      column.chunkColumn = false;
    } else if ("NVARCHAR2".equalsIgnoreCase(dataType)) {
      column.type = ChunkColumnValue.CHAR;
      column.chunkColumn = false;
    } else if ("VARCHAR2".equalsIgnoreCase(dataType)) {
      column.type = ChunkColumnValue.CHAR;
      column.chunkColumn = false;
    } else if ("LONG".equalsIgnoreCase(dataType)) {
      column.type = ChunkColumnValue.LONG;
      column.chunkColumn = true;
    } else if (PATTERN_LONG_RAW.matcher(dataType).matches()) {
      column.type = ChunkColumnValue.LONGRAW;
      column.chunkColumn = true;
    } else if ("FLOAT".equalsIgnoreCase(dataType)) {
      column.type = ColumnValue.NUMBER;
      column.chunkColumn = false;
    } else {
      throw new SQLException(
          String.format(
              "Column '%s' is has an unsupported Oracle Type. Report this test case \"%s\", %s, %s.",
              column.name,
              dataType,
              column.precision,
              column.scale
          )
      );
    }

    return column;
  }

  class DefaultColumn implements Column {
    String name;
    int type;
    String comments;
    Integer scale;
    Integer precision;
    boolean nullable;
    boolean chunkColumn;

    @Override
    public String name() {
      return this.name;
    }

    @Override
    public int type() {
      return this.type;
    }

    @Override
    public String comments() {
      return this.comments;
    }

    @Override
    public Integer scale() {
      return this.scale;
    }

    @Override
    public Integer precision() {
      return this.precision;
    }

    @Override
    public boolean nullable() {
      return this.nullable;
    }

    @Override
    public boolean chunkColumn() {
      return this.chunkColumn;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("name", this.name)
          .add("type", this.type)
          .add("comments", this.comments)
          .add("scale", this.scale)
          .add("precision", this.precision)
          .add("nullable", this.nullable)
          .add("chunkColumn", this.chunkColumn)
          .toString();
    }
  }
}
