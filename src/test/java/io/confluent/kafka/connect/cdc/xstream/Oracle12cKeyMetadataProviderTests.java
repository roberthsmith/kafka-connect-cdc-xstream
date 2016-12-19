package io.confluent.kafka.connect.cdc.xstream;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import io.confluent.kafka.connect.cdc.xstream.docker.healthcheck.OracleHealthCheck;
import org.flywaydb.core.Flyway;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class Oracle12cKeyMetadataProviderTests {
  private static final Logger log = LoggerFactory.getLogger(Oracle12cKeyMetadataProviderTests.class);
  static final String USERNAME = "system";
  static final String PASSWORD = "oracle";

  @ClassRule
  public static DockerComposeRule docker = DockerComposeRule.builder()
      .file("src/test/resources/docker-compose-12c.yml")
      .waitingForService("oracle12c", new OracleHealthCheck(USERNAME, PASSWORD, "ORCL"))
      .saveLogsTo("target/oracle12c")
      .build();

  Connection connection;
  Oracle11gKeyMetadataProvider keyMetadataProvider;


  @Before
  public void before() throws SQLException {
    Container container = docker.containers().container("oracle12c");
    DockerPort oraclePort = container.port(1521);
    final String jdbcUrl = oraclePort.inFormat("jdbc:oracle:thin:@$HOST:$EXTERNAL_PORT:orcl");

    this.connection = Utils.openConnection(jdbcUrl, USERNAME, PASSWORD);

    DatabaseMetaData metaData = this.connection.getMetaData();

    if (log.isInfoEnabled()) {
      log.info("Connected to Oracle version {}.{} on url {}",
          metaData.getDatabaseMajorVersion(),
          metaData.getDatabaseMinorVersion(),
          jdbcUrl
      );
    }

    Flyway flyway = new Flyway();
    flyway.setDataSource(jdbcUrl, USERNAME, PASSWORD);
    flyway.setSchemas("cdc_testing");
    flyway.migrate();

    this.keyMetadataProvider = new Oracle11gKeyMetadataProvider(this.connection);
  }

//  @Test
//  public void findPrimaryKey() throws SQLException {
//    Set<String> expectedKeys = ImmutableSet.of("USER_ID");
//    Set<String> actualKeys = this.keyMetadataProvider.findPrimaryKey("CDC_TESTING", "PRIMARY_KEY_TABLE");
//    assertThat("actualKeys did not match.",  actualKeys, IsEqual.equalTo(expectedKeys));
//
//    expectedKeys = ImmutableSet.of();
//    actualKeys = this.keyMetadataProvider.findPrimaryKey("CDC_TESTING", "UNIQUE_INDEX_TABLE");
//    assertThat("actualKeys did not match.",  actualKeys, IsEqual.equalTo(expectedKeys));
//  }
//
//  @Test
//  public void findUniqueKey() throws SQLException {
//    Set<String> expectedKeys = ImmutableSet.of("FIRST_COLUMN", "SECOND_COLUMN");
//    Set<String> actualKeys = this.keyMetadataProvider.findUniqueKey("cdc_testing", "UNIQUE_INDEX_TABLE");
//    assertThat("actualKeys did not match.",  actualKeys, IsEqual.equalTo(expectedKeys));
//
//    expectedKeys = ImmutableSet.of();
//    actualKeys = this.keyMetadataProvider.findUniqueKey("CDC_TESTING", "NO_INDEXES");
//    assertThat("actualKeys did not match.",  actualKeys, IsEqual.equalTo(expectedKeys));
//  }
//
//  @Test
//  public void findKeys() throws SQLException {
//    Set<String> expectedKeys = ImmutableSet.of("FIRST_COLUMN", "SECOND_COLUMN");
//    Set<String> actualKeys = this.keyMetadataProvider.findKeys("cdc_testing", "UNIQUE_INDEX_TABLE");
//    assertThat("actualKeys did not match.",  actualKeys, IsEqual.equalTo(expectedKeys));
//  }

  @AfterAll
  public static void afterClass() {
    docker.after();
  }

}
