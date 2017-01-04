package io.confluent.kafka.connect.cdc.xstream;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import io.confluent.kafka.connect.cdc.JdbcUtils;
import oracle.jdbc.OracleConnection;
import oracle.streams.XStreamOut;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueryService extends AbstractExecutionThreadService {
  private static final Logger log = LoggerFactory.getLogger(QueryService.class);
  final XStreamSourceConnectorConfig config;

  Time time = new SystemTime();
  OracleConnection connection;
  XStreamOut xStreamOut;

  QueryService(XStreamSourceConnectorConfig config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    this.connection = OracleUtils.openConnection(this.config);

    if(log.isInfoEnabled()) {

    }

    byte[] offset = new byte[10];

    String xStreamServerName = this.config.xStreamServerNames.get(0);

    this.xStreamOut = XStreamOut.attach(
        this.connection,
        xStreamServerName,
        offset,
        this.config.xStreamBatchInterval,
        this.config.xStreamIdleTimeout,
        XStreamOut.DEFAULT_MODE
    );



  }

  @Override
  protected void run() throws Exception {
    while(isRunning()) {
//      try {
//        LCR lcr = this.xStreamOut.receiveLCR(XStreamOut.DEFAULT_MODE);
//
//        if(null==lcr) {
//          continue;
//        }
//
//        if(log.isTraceEnabled()) {
//          log.trace("LCR received. {}", lcr);
//        }
//
//        if(!(lcr instanceof RowLCR)) {
//          if(log.isDebugEnabled()) {
//            log.debug("Skipping LCR because it is not a row.");
//          }
//          continue;
//        }
//
//
//
//
//      } catch (SQLException ex) {
//
//      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    JdbcUtils.closeConnection(this.connection);
  }
}
