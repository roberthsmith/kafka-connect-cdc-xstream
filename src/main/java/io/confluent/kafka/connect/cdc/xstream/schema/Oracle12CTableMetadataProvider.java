package io.confluent.kafka.connect.cdc.xstream.schema;

import com.google.common.base.Preconditions;
import oracle.streams.RowLCR;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

class Oracle12CTableMetadataProvider extends TableMetadataProvider {
  static final Logger log = LoggerFactory.getLogger(Oracle12CTableMetadataProvider.class);
  final PreparedStatement columnStatement;
  final PreparedStatement keyStatement;
  final String instanceName;
  final String conName;
  static final String ROOT = "CDB$ROOT";

  protected Oracle12CTableMetadataProvider(Connection connection) throws SQLException {
    super(connection);

    try (PreparedStatement statement = prepareSQLResource("12.detect.instance.sql")) {
      try (ResultSet resultSet = statement.executeQuery()) {
        Preconditions.checkState(resultSet.next());
        this.instanceName = resultSet.getString("DB_NAME");
        this.conName = resultSet.getString("CON_NAME");
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("instanceName = {} conName = {}", this.instanceName, this.conName);
    }

    this.columnStatement = prepareSQLResource("12.columns.sql");
    this.keyStatement = prepareSQLResource("12.keys.sql");
  }

  void changeContainer(RowLCR rowLCR) throws SQLException {
    String container = instanceName.equalsIgnoreCase(rowLCR.getSourceDatabaseName()) ? ROOT : rowLCR.getSourceDatabaseName();
    String sqlTemplate = loadSQLResource("12.container.sql");
    String sql = String.format(sqlTemplate, container);
    if (log.isDebugEnabled()) {
      log.debug("Executing {}", sql);
    }
    try (Statement statement = this.connection.createStatement()) {
      statement.execute(sql);
    }
    if (log.isDebugEnabled()) {
      log.debug("Changing container to {}", container);
    }
  }

  @Override
  public List<Column> getColumns(RowLCR row) throws SQLException {
    changeContainer(row);

    if (log.isDebugEnabled()) {
      log.debug("Calling getColumns('{}', '{}', '{}');",
          row.getSourceDatabaseName(),
          row.getObjectOwner(),
          row.getObjectName()
      );
    }

    this.columnStatement.setString(1, row.getObjectOwner());
    this.columnStatement.setString(2, row.getObjectName());
    this.columnStatement.setString(3, row.getSourceDatabaseName());

    List<Column> columns = new ArrayList<>();
    try (ResultSet resultSet = this.columnStatement.executeQuery()) {
      while (resultSet.next()) {
        columns.add(column(resultSet));
      }
    }
    return columns;
  }

  @Override
  public List<String> getKeys(RowLCR row) throws SQLException {
    changeContainer(row);

    if (log.isDebugEnabled()) {
      log.debug("Calling getKeys('{}', '{}', '{}');",
          row.getSourceDatabaseName(),
          row.getObjectOwner(),
          row.getObjectName()
      );
    }

    this.keyStatement.setString(1, row.getObjectOwner());
    this.keyStatement.setString(2, row.getObjectName());
    this.keyStatement.setString(3, row.getSourceDatabaseName());
    Set<String> columns = new LinkedHashSet<String>();
    try (ResultSet resultSet = this.keyStatement.executeQuery()) {
      while (resultSet.next()) {
        String columnName = resultSet.getString("COLUMN_NAME");
        columns.add(columnName);
      }
    }
    return new ArrayList<>(columns);
  }
}
