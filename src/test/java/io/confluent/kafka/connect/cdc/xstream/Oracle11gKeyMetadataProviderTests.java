package io.confluent.kafka.connect.cdc.xstream;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.execution.DockerComposeExecArgument;
import com.palantir.docker.compose.execution.DockerComposeExecOption;
import io.confluent.kafka.connect.cdc.xstream.docker.healthcheck.OracleHealthCheck;
import oracle.jdbc.OracleConnection;
import org.apache.kafka.connect.errors.DataException;
import org.flywaydb.core.Flyway;
import org.hamcrest.core.IsEqual;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertThat;

public class Oracle11gKeyMetadataProviderTests {
  private static final Logger log = LoggerFactory.getLogger(Oracle11gKeyMetadataProviderTests.class);
  static final String USERNAME = "system";
  static final String PASSWORD = "oracle";
  static final String ORACLE_INSTANCE = "ORCL";
  static final String ORACLE_CONTAINER = "oracle11g";
  static final String JDBC_URL_FORMAT = "jdbc:oracle:thin:@$HOST:$EXTERNAL_PORT:" + ORACLE_INSTANCE;

  @ClassRule
  public final static DockerComposeRule docker = DockerComposeRule.builder()
      .file("src/test/resources/docker-compose-11g.yml")
      .waitingForService(ORACLE_CONTAINER, new OracleHealthCheck(USERNAME, PASSWORD, ORACLE_INSTANCE))
      .saveLogsTo("target/oracle11g")
      .build();

  static Container oracleContainer;

  Connection connection;
  Oracle11gKeyMetadataProvider keyMetadataProvider;

  static String jdbcUrl;


  @BeforeClass
  public static void beforeClass() throws SQLException, InterruptedException, IOException {
    oracleContainer = docker.containers().container(ORACLE_CONTAINER);
    DockerPort oraclePort = oracleContainer.port(1521);
    jdbcUrl = oraclePort.inFormat(JDBC_URL_FORMAT);
    if (log.isInfoEnabled()) {
      log.info("Setting jdbcUrl to {}", jdbcUrl);
    }

    configureOracleLogging();
    flywayMigrate();
  }

  static void executeStatements(OracleConnection connection, List<String> statements) throws SQLException {
    try (Statement statement = connection.createStatement()) {

      for (String sql : statements) {
        if (log.isInfoEnabled()) {
          log.info("Executing SQL: {}", sql);
        }

        statement.executeUpdate(sql);
      }
    }
  }

  static void configureOracleLogging() throws SQLException, InterruptedException, IOException {
    DockerComposeExecArgument execArgument = DockerComposeExecArgument.arguments(
        "bash",
        "-c",
        "ORACLE_HOME=/opt/oracle/app/product/11.2.0/dbhome_1 ORACLE_SID=orcl /opt/oracle/app/product/11.2.0/dbhome_1/bin/sqlplus system/oracle as sysdba @/db/init/startup.sql"
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
    flyway.setDataSource(jdbcUrl, USERNAME, PASSWORD);
    flyway.setSchemas("CDC_TESTING");
    flyway.migrate();
  }

  @Before
  public void before() throws SQLException {
    this.connection = Utils.openConnection(jdbcUrl, USERNAME, PASSWORD);
    this.keyMetadataProvider = new Oracle11gKeyMetadataProvider(this.connection);
  }

  @Test
  public void findPrimaryKey() throws SQLException {
    Set<String> expectedKeys = ImmutableSet.of("USER_ID");
    Set<String> actualKeys = this.keyMetadataProvider.findPrimaryKey("CDC_TESTING", "PRIMARY_KEY_TABLE");
    assertThat("actualKeys did not match.", actualKeys, IsEqual.equalTo(expectedKeys));

    expectedKeys = ImmutableSet.of();
    actualKeys = this.keyMetadataProvider.findPrimaryKey("CDC_TESTING", "UNIQUE_INDEX_TABLE");
    assertThat("actualKeys did not match.", actualKeys, IsEqual.equalTo(expectedKeys));
  }

  @Test
  public void findUniqueKey() throws SQLException {
    Set<String> expectedKeys = ImmutableSet.of("FIRST_COLUMN", "SECOND_COLUMN");
    Set<String> actualKeys = this.keyMetadataProvider.findUniqueKey("cdc_testing", "UNIQUE_INDEX_TABLE");
    assertThat("actualKeys did not match.", actualKeys, IsEqual.equalTo(expectedKeys));

    expectedKeys = ImmutableSet.of();
    actualKeys = this.keyMetadataProvider.findUniqueKey("CDC_TESTING", "NO_INDEXES");
    assertThat("actualKeys did not match.", actualKeys, IsEqual.equalTo(expectedKeys));
  }

  @Test
  public void findKeys() throws SQLException {
    Set<String> expectedKeys = ImmutableSet.of("FIRST_COLUMN", "SECOND_COLUMN");
    Set<String> actualKeys = this.keyMetadataProvider.findKeys("cdc_testing", "UNIQUE_INDEX_TABLE");
    assertThat("actualKeys did not match.", actualKeys, IsEqual.equalTo(expectedKeys));
  }

  @AfterClass
  public static void afterClass() {
    docker.after();
  }

}
