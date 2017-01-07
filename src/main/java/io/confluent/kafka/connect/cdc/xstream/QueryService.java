package io.confluent.kafka.connect.cdc.xstream;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.ChangeWriter;
import io.confluent.kafka.connect.cdc.JdbcUtils;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import oracle.jdbc.OracleConnection;
import oracle.streams.LCR;
import oracle.streams.RowLCR;
import oracle.streams.StreamsException;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class QueryService extends AbstractExecutionThreadService {
  private static final Logger log = LoggerFactory.getLogger(QueryService.class);
  final OracleSourceConnectorConfig config;
  final OffsetStorageReader offsetStorageReader;
  final ChangeWriter changeWriter;
  OracleChange.Builder oracleChangeBuilder;
  OracleConnection connection;
  XStreamOutput xStreamOutput;
  TableMetadataProvider tableMetadataProvider;
  CountDownLatch finished = new CountDownLatch(1);

  QueryService(OracleSourceConnectorConfig config, OffsetStorageReader offsetStorageReader, ChangeWriter changeWriter) {
    this.config = config;
    this.offsetStorageReader = offsetStorageReader;
    this.changeWriter = changeWriter;
  }

  @Override
  protected void startUp() throws Exception {
    this.connection = OracleUtils.openUnPooledConnection(this.config);

    DatabaseMetaData databaseMetaData = this.connection.getMetaData();

    if (log.isInfoEnabled()) {
      log.info("Connected to Oracle {}.{}", databaseMetaData.getDatabaseMajorVersion(), databaseMetaData.getDatabaseMinorVersion());
    }

    switch (databaseMetaData.getDatabaseMajorVersion()) {
      case 12:
        this.tableMetadataProvider = new Oracle12cTableMetadataProvider(this.config, this.offsetStorageReader);
        break;
      case 11:
        this.tableMetadataProvider = new Oracle11gTableMetadataProvider(this.config, this.offsetStorageReader);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported Oracle version. %d.%d.", databaseMetaData.getDatabaseMajorVersion(), databaseMetaData.getDatabaseMinorVersion())
        );
    }

    //TODO: Get the last position. For now we are always starting from the beginning of time.
    byte[] position = null;
    this.xStreamOutput = XStreamOutputImpl.attach(this.connection, this.config, position);
    this.oracleChangeBuilder = new OracleChange.Builder(this.config, this.xStreamOutput, this.tableMetadataProvider);
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      try {
        OracleChange change = receiveChange();
        this.changeWriter.addChange(change);
      } catch (Exception ex) {
        if (log.isErrorEnabled()) {
          log.error("Exception thrown", ex);
        }
      }
    }
    finished.countDown();
  }


  protected OracleChange receiveChange() throws StreamsException, SQLException {
    OracleChange oracleChange = null;

    LCR lcr = this.xStreamOutput.receiveLCR();

    if (log.isTraceEnabled()) {
      log.trace("lcr = {}", lcr);
    }

    if (lcr instanceof RowLCR) {
      RowLCR rowLCR = (RowLCR) lcr;

      ChangeKey changeKey = new ChangeKey(rowLCR.getSourceDatabaseName(), rowLCR.getObjectOwner(), rowLCR.getObjectName());
      if (this.config.allowedCommands.contains(lcr.getCommandType())) {
        oracleChange = this.oracleChangeBuilder.build(rowLCR);
      } else {
        if (log.isTraceEnabled()) {
          log.trace("{}: Skipping RowLCR because commandType('{}') is not allowed.", changeKey, rowLCR.getCommandType());
        }
      }
    } else {
      if (log.isTraceEnabled()) {
        log.trace("LCR is not a RowLCR.");
      }
    }

    return oracleChange;
  }


  @Override
  protected void shutDown() throws Exception {
    if (log.isInfoEnabled()) {
      log.info("Shutting down. Waiting for loop to complete.");
    }
    if(!finished.await(60, TimeUnit.SECONDS)){
      log.warn("Took over {} seconds to shutdown.", 60);
    }
    this.xStreamOutput.detach();
    JdbcUtils.closeConnection(this.connection);
  }
}
