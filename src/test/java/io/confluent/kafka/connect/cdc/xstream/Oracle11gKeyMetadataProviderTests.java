package io.confluent.kafka.connect.cdc.xstream;

import com.google.common.collect.ImmutableSet;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.execution.DockerComposeExecArgument;
import com.palantir.docker.compose.execution.DockerComposeExecOption;
import org.flywaydb.core.Flyway;
import org.hamcrest.core.IsEqual;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import static org.junit.Assert.assertThat;

@Ignore
public class Oracle11gKeyMetadataProviderTests {
  private static final Logger log = LoggerFactory.getLogger(Oracle11gKeyMetadataProviderTests.class);

  @ClassRule
  public final static DockerComposeRule docker = DockerUtils.oracle11g();
  public static Container oracleContainer;
  public static String jdbcUrl;

  Connection connection;
  Oracle11gKeyMetadataProvider keyMetadataProvider;

  @BeforeClass
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
        "ORACLE_HOME=/opt/oracle/app/product/11.2.0/dbhome_1 ORACLE_SID=orcl /opt/oracle/app/product/11.2.0/dbhome_1/bin/sqlplus system/oracle as sysdba @/db/init/11g.startup.sql"
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

  @Before
  public void before() throws SQLException {
    this.connection = Utils.openConnection(jdbcUrl, DockerUtils.USERNAME, DockerUtils.PASSWORD);
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
