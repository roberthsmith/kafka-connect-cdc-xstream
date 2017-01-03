package db.migration.oracle12c;

import io.confluent.kafka.connect.cdc.xstream.Oracle12cTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class V0_0_1_99__CreateOutbound extends BaseJdbcMigration {
  private static Logger log = LoggerFactory.getLogger(V0_0_1_99__CreateOutbound.class);
  @Override
  public void migrate(Connection connection) throws Exception {

//    if(log.isDebugEnabled()){
//      return;
//    }
//
//    Thread thread = new Thread(new Runnable() {
//      @Override
//      public void run() {
//        try {
//          Oracle12cTest.createOutbound();
//        } catch (InterruptedException ex) {
//          if(log.isTraceEnabled()) {
//            log.trace("Thread was interrupted.", ex);
//          }
//        } catch (Exception ex) {
//          if(log.isErrorEnabled()) {
//            log.error("Exception thrown", ex);
//          }
//        }
//      }
//    });
//    thread.start();
//
//    try {
//      changeContainer(connection, "CDB$ROOT");
//
//      final String SERVER_NAME = "XOUT";
//      final String SQL = "SELECT SERVER_NAME FROM ALL_XSTREAM_OUTBOUND WHERE SERVER_NAME = ?";
//
//      try(PreparedStatement preparedStatement = connection.prepareStatement(SQL)) {
//        preparedStatement.setString(1, SERVER_NAME);
//        boolean serverFound=false;
//
//        while(!serverFound) {
//          try (ResultSet resultSet = preparedStatement.executeQuery()) {
//            if(log.isTraceEnabled()) {
//              log.trace("Checking for xStream outbound '{}'", SERVER_NAME);
//            }
//            while (resultSet.next()) {
//              String serverName = resultSet.getString(1);
//
//              if (log.isDebugEnabled()) {
//                log.debug("serverName = '{}'", serverName);
//              }
//
//              if (SERVER_NAME.equals(serverName)) {
//                if (log.isInfoEnabled()) {
//                  log.info("xStream outbound '{}' found.", SERVER_NAME);
//                }
//                serverFound = true;
//                thread.interrupt();
//                break;
//              } else {
//                if (log.isInfoEnabled()) {
//                  log.info("xStream outbound '{}' was found but is not enabled yet.", serverName);
//                }
//              }
//            }
//          }
//          if(!serverFound) {
//            if(log.isTraceEnabled()) {
//              log.trace("xStream outbound '{}' was not found.", SERVER_NAME);
//            }
//            Thread.sleep(2500);
//          }
//        }
//      }
//    } finally {
//      changeContainer(connection, "ORCLPDB1");
//    }
  }



}
