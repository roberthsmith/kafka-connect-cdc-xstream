package io.confluent.kafka.connect.cdc.xstream;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.execution.DockerComposeExecArgument;
import com.palantir.docker.compose.execution.DockerComposeExecOption;
import org.flywaydb.core.Flyway;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public class Oracle12cTest {
  private static final Logger log = LoggerFactory.getLogger(Oracle12cTest.class);

  @ClassRule
  public final static DockerComposeRule docker = DockerUtils.oracle12c();
  public static Container oracleContainer;
  public static String jdbcUrl;

  @BeforeAll
  public static void beforeClass() throws SQLException, InterruptedException, IOException {
    oracleContainer = DockerUtils.oracleContainer(docker);
    jdbcUrl = DockerUtils.jdbcUrl(docker);

    configureOracleLogging();
    flywayMigrate();
  }

  static void configureOracleLogging() throws SQLException, InterruptedException, IOException {
    DockerComposeExecArgument execArgument = DockerComposeExecArgument.arguments(
        "bash",
        "-c",
        "ORACLE_HOME=/opt/oracle/app/product/12.1.0.2/dbhome_1 ORACLE_SID=orcl /opt/oracle/app/product/12.1.0.2/dbhome_1/bin/sqlplus sys/oracle as sysdba @/db/init/12c/12c.startup.sql"
    );
    DockerComposeExecOption execOptions = DockerComposeExecOption.options("--user", "oracle");

    if (log.isInfoEnabled()) {
      log.info("Executing command with {} {}", execOptions, execArgument);
    }

    String dockerExecOutput = docker.exec(
        execOptions,
        oracleContainer.getContainerName(),
        execArgument
    );

    if (log.isInfoEnabled()) {
      log.info("docker exec output\n{}", dockerExecOutput);
    }
  }

  static void flywayMigrate() throws SQLException {
    Flyway flyway = new Flyway();
    flyway.setDataSource(jdbcUrl, DockerUtils.USERNAME, DockerUtils.PASSWORD);
    flyway.setSchemas("CDC_TESTING");
    flyway.migrate();
  }

  @AfterEach
  public static void dockerCleanup() {
    docker.after();
  }
}
