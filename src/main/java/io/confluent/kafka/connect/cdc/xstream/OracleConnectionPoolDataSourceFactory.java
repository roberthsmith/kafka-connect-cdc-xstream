package io.confluent.kafka.connect.cdc.xstream;

import com.google.common.base.Strings;
import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.ConnectionKey;
import io.confluent.kafka.connect.cdc.ConnectionPoolDataSourceFactory;
import oracle.jdbc.pool.OracleConnectionPoolDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.ConnectionPoolDataSource;
import java.sql.SQLException;

class OracleConnectionPoolDataSourceFactory implements ConnectionPoolDataSourceFactory {
  private static final Logger log = LoggerFactory.getLogger(OracleConnectionPoolDataSourceFactory.class);
  private final OracleSourceConnectorConfig config;

  public OracleConnectionPoolDataSourceFactory(OracleSourceConnectorConfig config) {
    this.config = config;
  }

  @Override
  public ConnectionPoolDataSource connectionPool(ConnectionKey connectionKey) throws SQLException {
    OracleConnectionPoolDataSource a = new OracleConnectionPoolDataSource();
    a.setUser(this.config.jdbcUsername);
    a.setDriverType("oci");
    a.setPassword(this.config.jdbcPassword);
    a.setServerName(this.config.serverName);
    a.setPortNumber(this.config.serverPort);

    if (!Strings.isNullOrEmpty(connectionKey.databaseName)) {
      a.setServiceName(connectionKey.databaseName);
    } else {
      a.setServiceName(this.config.initialDatabase);
    }

    if (log.isTraceEnabled()) {
      log.trace("{}: Computed JdbcUrl {}", connectionKey, a.getURL());
    }

    return a;
  }

  @Override
  public ConnectionKey connectionKey(ChangeKey changeKey) {
    String databaseName;
    if (null == changeKey) {
      databaseName = this.config.initialDatabase;
    } else {
      databaseName = changeKey.databaseName;
    }

    return ConnectionKey.of(this.config.serverName, this.config.serverPort, this.config.jdbcUsername, databaseName);
  }
}
