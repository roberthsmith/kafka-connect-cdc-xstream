package io.confluent.kafka.connect.cdc.xstream;

public class XStreamConstants {
  public static final String USERNAME = "system";
  public static final String PASSWORD = "oracle";
  public static final String ORACLE_CONTAINER = "oracle";
  public static final int ORACLE_PORT = 1521;
  public static final String JDBC_URL_FORMAT_11G = "jdbc:oracle:oci:@$HOST:$EXTERNAL_PORT:ORCL";
  public static final String JDBC_URL_FORMAT_12C_PDB = "jdbc:oracle:oci:@$HOST:$EXTERNAL_PORT/ORCLPDB1";
  public static final String JDBC_URL_FORMAT_12C_ROOT = "jdbc:oracle:oci:@$HOST:$EXTERNAL_PORT/ORCL";
  public static final String XSTREAM_USERNAME_11G = "xstrmadmin";
  public static final String XSTREAM_PASSWORD_11G = "lfnjgksdfbdk";
  public static final String XSTREAM_USERNAME_12C = "c##xstrmadmin";
  public static final String XSTREAM_PASSWORD_12C = "lfnjgksdfbdk";
}
