package io.confluent.kafka.connect.cdc.xstream;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import io.confluent.kafka.connect.cdc.docker.JdbcHealthCheck;

class DockerUtils {

  public static DockerComposeRule oracle11g() {
    return DockerComposeRule.builder()
        .file("src/test/resources/docker-compose-11g.yml")
        .waitingForService(Constants.ORACLE_CONTAINER, new JdbcHealthCheck(Constants.USERNAME, Constants.PASSWORD, 1521, Constants.JDBC_URL_FORMAT_11G))
        .saveLogsTo("target/oracle11g")
        .build();
  }

  public static DockerComposeRule oracle12c() {
    return io.confluent.kafka.connect.cdc.docker.DockerUtils.loadRule(
        "src/test/resources/docker-compose-12c.yml",
        "target/oracle12c",
        Constants.ORACLE_CONTAINER,
        new JdbcHealthCheck(Constants.USERNAME, Constants.PASSWORD, 1521, Constants.JDBC_URL_FORMAT_12C_PDB)
    );
  }

  public static Container oracleContainer(DockerComposeRule docker) {
    return docker.containers().container(Constants.ORACLE_CONTAINER);
  }

  public static DockerPort oraclePort(Container oracleContainer) {
    return oracleContainer.port(1521);
  }

  public static String jdbcUrl(DockerComposeRule docker, String jdbcFormat) {
    Container oracleContainer = oracleContainer(docker);
    DockerPort dockerPort = oraclePort(oracleContainer);
    return dockerPort.inFormat(jdbcFormat);
  }
}
