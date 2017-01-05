package io.confluent.kafka.connect.cdc.xstream;

import com.google.common.base.Preconditions;

import java.util.LinkedHashMap;
import java.util.Map;

public class XStreamTestConstants {
  public static final String USERNAME = "system";
  public static final String PASSWORD = "oracle";
  public static final String ORACLE_CONTAINER = "oracle";
  public static final int ORACLE_PORT = 1521;
  public static final String ORACLE_ROOT_DATABASE = "ORCL";
  public static final String ORACLE_PDB_DATABASE = "ORCLPDB1";
  public static final String XSTREAM_OUT_SERVER_NAME = "xout";

  public static final String JDBC_URL_FORMAT_11G = "jdbc:oracle:oci:@$HOST:$EXTERNAL_PORT/" + ORACLE_ROOT_DATABASE;
  public static final String JDBC_URL_FORMAT_12C_PDB = "jdbc:oracle:oci:@$HOST:$EXTERNAL_PORT/" + ORACLE_PDB_DATABASE;
  public static final String JDBC_URL_FORMAT_12C_ROOT = "jdbc:oracle:oci:@$HOST:$EXTERNAL_PORT/" + ORACLE_ROOT_DATABASE;
  public static final String XSTREAM_USERNAME_11G = "xstrmadmin";
  public static final String XSTREAM_PASSWORD_11G = "lfnjgksdfbdk";
  public static final String XSTREAM_USERNAME_12C = "c##xstrmadmin";
  public static final String XSTREAM_PASSWORD_12C = "lfnjgksdfbdk";

  public static Map<String, String> settings(String host, Integer port) {
    Preconditions.checkNotNull(host, "host cannot be null.");
    Preconditions.checkNotNull(port, "port cannot be null.");
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put(XStreamSourceConnectorConfig.SERVER_NAME_CONF, host);
    settings.put(XStreamSourceConnectorConfig.SERVER_PORT_CONF, port.toString());
    settings.put(XStreamSourceConnectorConfig.INITIAL_DATABASE_CONF, XStreamTestConstants.ORACLE_ROOT_DATABASE);
    settings.put(XStreamSourceConnectorConfig.JDBC_USERNAME_CONF, XStreamTestConstants.XSTREAM_USERNAME_12C);
    settings.put(XStreamSourceConnectorConfig.JDBC_PASSWORD_CONF, XStreamTestConstants.XSTREAM_PASSWORD_12C);
    settings.put(XStreamSourceConnectorConfig.XSTREAM_SERVER_NAMES_CONF, XStreamTestConstants.XSTREAM_OUT_SERVER_NAME);
    return settings;
  }
}
