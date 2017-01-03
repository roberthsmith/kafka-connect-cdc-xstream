package db.migration.oracle12c;

import org.flywaydb.core.api.migration.jdbc.JdbcMigration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class BaseJdbcMigration implements JdbcMigration {
  private static Logger log = LoggerFactory.getLogger(BaseJdbcMigration.class);

  protected void changeContainer(Connection connection, String container) throws SQLException {
    if(log.isInfoEnabled()) {
      log.info("Changing container to {}", container);
    }
    try(Statement statement = connection.createStatement()){
      statement.execute("ALTER SESSION SET CONTAINER = " + container);
    }
  }

}
