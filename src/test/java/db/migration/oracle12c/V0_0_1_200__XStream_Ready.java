package db.migration.oracle12c;

import io.confluent.kafka.connect.cdc.xstream.Oracle12cTest;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class V0_0_1_200__XStream_Ready extends BaseJdbcMigration {
  private static Logger log = LoggerFactory.getLogger(V0_0_1_200__XStream_Ready.class);



  @Override
  public void migrate(Connection connection) throws Exception {
    if(log.isDebugEnabled()){
      return;
    }

    try {
      changeContainer(connection, "CDB$ROOT");

      final String SERVER_NAME = "XOUT";
      final String SQL = "SELECT APPLY_NAME, STATUS FROM DBA_APPLY WHERE APPLY_NAME = ?";

      try(PreparedStatement preparedStatement = connection.prepareStatement(SQL)) {
        preparedStatement.setString(1, SERVER_NAME);

        boolean serverFound=false;

        while(!serverFound) {
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            if(log.isTraceEnabled()) {
              log.trace("Checking for xStream outbound '{}'", SERVER_NAME);
            }
            while (resultSet.next()) {
              String applyName = resultSet.getString(1);
              String status = resultSet.getString(2);

              if(log.isDebugEnabled()) {
                log.debug("applyName = '{}' status = '{}'", applyName, status);
              }

              final String ENABLED="ENABLED";
              if(SERVER_NAME.equals(applyName)) {
                if(ENABLED.equals(status)) {
                  if(log.isInfoEnabled()) {
                    log.info("xStream outbound '{}' found.", SERVER_NAME);
                  }
                  serverFound=true;
                  break;
                } else {
                  if(log.isInfoEnabled()) {
                    log.info("xStream outbound '{}' was found but is not enabled yet.", applyName);
                  }
                }
              }
            }
          }
          if(!serverFound) {
            if(log.isTraceEnabled()) {
              log.trace("xStream outbound '{}' was not found.", SERVER_NAME);
            }
            Thread.sleep(2500);
          }
        }
      }

    } finally {
      changeContainer(connection, "ORCLPDB1");
    }
  }
}
