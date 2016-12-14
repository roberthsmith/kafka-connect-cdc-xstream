package io.confluent.kafka.connect.cdc.xstream.docker.healthcheck;

import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.Attempt;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

public class OracleHealthCheck implements HealthCheck<Container> {
  private static final Logger log = LoggerFactory.getLogger(OracleHealthCheck.class);
  final String username;
  final String password;
  final String instance;

  public OracleHealthCheck(String username, String password, String instance) {
    this.username = username;
    this.password = password;
    this.instance = instance;
  }

  @Override
  public SuccessOrFailure isHealthy(Container container) {
    String formatJdbcUrlFormat = String.format("jdbc:oracle:thin:@$HOST:$EXTERNAL_PORT:%s", this.instance);
    DockerPort oraclePort = container.port(1521);
    final String jdbcUrl = oraclePort.inFormat(formatJdbcUrlFormat);

    if (log.isInfoEnabled()) {
      log.info("Attempting to authenticate to {} with user {}.", jdbcUrl, this.username);
    }

    return SuccessOrFailure.onResultOf(
        new Attempt() {
          @Override
          public boolean attempt() throws Exception {
            try (Connection connection = DriverManager.getConnection(
                jdbcUrl,
                username,
                password
            )) {
              return true;
            } catch (Exception ex) {
              if(log.isDebugEnabled()) {
                log.debug("Exception thrown", ex);
              }

              Thread.sleep(2000);
              return false;
            }

          }
        }
    );
  }
}
