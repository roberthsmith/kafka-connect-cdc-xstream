package io.confluent.kafka.connect.cdc.xstream.docker;

import io.confluent.kafka.connect.cdc.xstream.Constants;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class Oracle12cClusterHealthCheck extends OracleClusterHealthCheck {
  public Oracle12cClusterHealthCheck() {
    super(Constants.JDBC_URL_FORMAT_12C_PDB, Constants.XSTREAM_USERNAME_12C, Constants.XSTREAM_PASSWORD_12C);
  }

  @Override
  protected boolean checkXStream(Connection connection) throws Exception {
    try (Statement statement = connection.createStatement()) {
      statement.execute("ALTER SESSION SET CONTAINER = CDB$ROOT");
    }

    final String SERVER_NAME = "XOUT";
    final String ENABLED = "ENABLED";
    final String SQL = "SELECT APPLY_NAME, STATUS FROM DBA_APPLY WHERE APPLY_NAME = ?";

    try (PreparedStatement preparedStatement = connection.prepareStatement(SQL)) {
      preparedStatement.setString(1, SERVER_NAME);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        if (log.isTraceEnabled()) {
          log.trace("Checking for xStream outbound '{}'", SERVER_NAME);
        }
        while (resultSet.next()) {
          String applyName = resultSet.getString(1);
          String status = resultSet.getString(2);

          if (log.isDebugEnabled()) {
            log.debug("applyName = '{}' status = '{}'", applyName, status);
          }

          if (SERVER_NAME.equals(applyName) && ENABLED.equals(status)) {
            return true;
          }
        }
      }
    }

    Thread.sleep(1000);
    return false;
  }
}
