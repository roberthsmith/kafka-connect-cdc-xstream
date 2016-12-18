package io.confluent.kafka.connect.cdc.xstream;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import io.confluent.kafka.connect.cdc.xstream.docker.healthcheck.OracleHealthCheck;

class DockerUtils {
  public static final String USERNAME = "system";
  public static final String PASSWORD = "oracle";
  public static final String ORACLE_INSTANCE = "ORCL";
  public static final String ORACLE_CONTAINER = "oracle";
  public static final String JDBC_URL_FORMAT = "jdbc:oracle:oci:@$HOST:$EXTERNAL_PORT:" + ORACLE_INSTANCE;

  public static final String XSTREAM_USERNAME="xstrmadmin";
  public static final String XSTREAM_PASSWORD="xstrmadmin";

  public static DockerComposeRule oracle11g() {
    return DockerComposeRule.builder()
        .file("src/test/resources/docker-compose-11g.yml")
        .waitingForService(ORACLE_CONTAINER, new OracleHealthCheck(USERNAME, PASSWORD, ORACLE_INSTANCE))
        .saveLogsTo("target/oracle11g")
        .build();
  }

  public static DockerComposeRule oracle12c() {
    return DockerComposeRule.builder()
        .file("src/test/resources/docker-compose-12c.yml")
        .waitingForService(ORACLE_CONTAINER, new OracleHealthCheck(USERNAME, PASSWORD, ORACLE_INSTANCE))
        .saveLogsTo("target/oracle12c")
        .build();
  }

  public static Container oracleContainer(DockerComposeRule docker) {
    return docker.containers().container(ORACLE_CONTAINER);
  }

  public static DockerPort oraclePort(Container oracleContainer) {
    return oracleContainer.port(1521);
  }

  public static String jdbcUrl(DockerComposeRule docker) {
    Container oracleContainer = oracleContainer(docker);
    DockerPort dockerPort = oraclePort(oracleContainer);
    return dockerPort.inFormat(JDBC_URL_FORMAT);
  }

}
