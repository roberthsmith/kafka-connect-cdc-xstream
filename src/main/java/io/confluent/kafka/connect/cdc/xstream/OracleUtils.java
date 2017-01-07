package io.confluent.kafka.connect.cdc.xstream;

import io.confluent.kafka.connect.cdc.JdbcUtils;
import oracle.jdbc.OracleConnection;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

class OracleUtils {
  private static final Logger log = LoggerFactory.getLogger(OracleUtils.class);

  public static OracleConnection openUnPooledConnection(OracleSourceConnectorConfig config) {
    try {
      return (OracleConnection) JdbcUtils.openPooledConnection(config, null).getConnection();
    } catch (UnsatisfiedLinkError ex) {
      if (log.isErrorEnabled()) {
        log.error("This exception is thrown when a ");
      }
      //TODO: Put together a nice message talking about troubleshooting.
      throw new ConnectException("Exception thrown while connecting to oracle.", ex);
    } catch (SQLException ex) {
      throw new ConnectException("Exception thrown while connecting to oracle.", ex);

    }
  }
}
