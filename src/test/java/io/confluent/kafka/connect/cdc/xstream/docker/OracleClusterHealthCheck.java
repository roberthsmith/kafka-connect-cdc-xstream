package io.confluent.kafka.connect.cdc.xstream.docker;

import com.palantir.docker.compose.connection.Cluster;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.Attempt;
import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import io.confluent.kafka.connect.cdc.xstream.XStreamTestConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class OracleClusterHealthCheck implements ClusterHealthCheck {
  static final Logger log = LoggerFactory.getLogger(OracleClusterHealthCheck.class);

  final String jdbcUrlFormat;
  final String username;
  final String password;

  public OracleClusterHealthCheck(String jdbcUrlFormat, String username, String password) {
    this.jdbcUrlFormat = jdbcUrlFormat;
    this.username = username;
    this.password = password;
  }

  protected abstract boolean checkXStream(Connection connection) throws SQLException, Exception;

  @Override
  public SuccessOrFailure isClusterHealthy(Cluster cluster) {
    return SuccessOrFailure.onResultOf(new Attempt() {
      @Override
      public boolean attempt() throws Exception {
        Container oracleContainer = cluster.container(XStreamTestConstants.ORACLE_CONTAINER);
        DockerPort dockerPort = oracleContainer.port(XStreamTestConstants.ORACLE_PORT);
        if (!dockerPort.isListeningNow()) {
          if (log.isTraceEnabled()) {
            log.trace("Port {} is not listening on container {}.", XStreamTestConstants.ORACLE_PORT, XStreamTestConstants.ORACLE_CONTAINER);
          }
          return false;
        }

        String jdbcUrl = dockerPort.inFormat(jdbcUrlFormat);
        if (log.isTraceEnabled()) {
          log.trace("Connecting to oracle instance on {}", jdbcUrl);
        }
        try (Connection connection = DriverManager.getConnection(
            jdbcUrl,
            username,
            password
        )) {
          return checkXStream(connection);
        } catch (Exception ex) {
          if (log.isTraceEnabled()) {
            log.trace("Exception thrown", ex);
          }

          Thread.sleep(2500);
          return false;
        }
      }
    });
  }
}
