package io.confluent.kafka.connect.cdc.xstream;

import io.confluent.kafka.connect.cdc.docker.DockerFormatString;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public class Oracle11gTest {
  private static final Logger log = LoggerFactory.getLogger(Oracle11gTest.class);

  @BeforeAll
  public static void beforeClass(@DockerFormatString(container = XStreamTestConstants.ORACLE_CONTAINER, port = XStreamTestConstants.ORACLE_PORT, format = XStreamTestConstants.JDBC_URL_FORMAT_11G) String jdbcUrl) throws SQLException, InterruptedException, IOException {
    flywayMigrate(jdbcUrl);
  }

  static void flywayMigrate(String jdbcUrl) throws SQLException {
    Flyway flyway = new Flyway();
    flyway.setDataSource(jdbcUrl, XStreamTestConstants.USERNAME, XStreamTestConstants.PASSWORD);
    flyway.setSchemas("CDC_TESTING");
    flyway.setLocations("db/migration/common", "db/migration/oracle11g");
    flyway.migrate();
  }
}
