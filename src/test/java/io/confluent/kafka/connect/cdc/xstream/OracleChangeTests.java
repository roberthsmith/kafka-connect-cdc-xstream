package io.confluent.kafka.connect.cdc.xstream;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.cdc.Integration;
import io.confluent.kafka.connect.cdc.JsonChange;
import io.confluent.kafka.connect.cdc.JsonTableMetadata;
import io.confluent.kafka.connect.cdc.NamedTest;
import io.confluent.kafka.connect.cdc.ObjectMapperFactory;
import io.confluent.kafka.connect.cdc.TestDataUtils;
import io.confluent.kafka.connect.cdc.docker.DockerCompose;
import io.confluent.kafka.connect.cdc.docker.DockerFormatString;
import io.confluent.kafka.connect.cdc.xstream.docker.Oracle12cClusterHealthCheck;
import io.confluent.kafka.connect.cdc.xstream.model.JsonRowLCR;
import io.confluent.kafka.connect.cdc.xstream.model.TableMetadataTestCase;
import oracle.streams.ChunkColumnValue;
import oracle.streams.RowLCR;
import oracle.streams.StreamsException;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Stream;

import static io.confluent.kafka.connect.cdc.ChangeAssertions.assertChange;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(Integration.class)
@DockerCompose(dockerComposePath = Oracle12cTests.DOCKER_COMPOSE_FILE, clusterHealthCheck = Oracle12cClusterHealthCheck.class)
public class OracleChangeTests extends Oracle12cTests {
  private static final Logger log = LoggerFactory.getLogger(OracleChangeTests.class);

  Connection connection;

  @BeforeEach
  public void openConnection(
      @DockerFormatString(container = XStreamConstants.ORACLE_CONTAINER, port = XStreamConstants.ORACLE_PORT, format = XStreamConstants.JDBC_URL_FORMAT_12C_PDB) String jdbcUrl
  ) throws SQLException {
    Map<String, String> settings = ImmutableMap.of(
        XStreamSourceConnectorConfig.JDBC_URL_CONF, jdbcUrl,
        XStreamSourceConnectorConfig.JDBC_USERNAME_CONF, XStreamConstants.XSTREAM_USERNAME_12C,
        XStreamSourceConnectorConfig.JDBC_PASSWORD_CONF, XStreamConstants.XSTREAM_PASSWORD_12C,
        XStreamSourceConnectorConfig.XSTREAM_SERVER_NAMES_CONF, "xout"
    );

    this.connection = DriverManager.getConnection(
        jdbcUrl,
        XStreamConstants.USERNAME,
        XStreamConstants.PASSWORD
    );
  }

  @AfterEach
  public void cleanup() throws SQLException {
    this.connection.close();
  }

  @Disabled
  @Test
  public void foo() throws IOException, StreamsException {
    String lcrPackageName = this.getClass().getPackage().getName() + ".lcrs";
    List<JsonRowLCR> jsonRowLCRS = TestDataUtils.loadJsonResourceFiles(lcrPackageName, JsonRowLCR.class);
    String tableMetadataPackageName = this.getClass().getPackage().getName() + ".tablemetadata";
    List<TableMetadataTestCase> tableMetadatas = TestDataUtils.loadJsonResourceFiles(tableMetadataPackageName, TableMetadataTestCase.class);
    Map<String, TableMetadataTestCase> tableMetadataMap = new HashMap<>();
    for (TableMetadataTestCase tableMetadata : tableMetadatas) {
      tableMetadataMap.put(tableMetadata.tableName().toUpperCase(), tableMetadata);
    }

    for (JsonRowLCR rowLCR : jsonRowLCRS) {
      String objectName = rowLCR.getObjectName().toUpperCase();

      if (RowLCR.DELETE.equals(rowLCR.getCommandType())) {
        continue;
      }

      if (!tableMetadataMap.containsKey(objectName)) {
        continue;
      }

      String fileName = String.format("%s.json", rowLCR.name());

      Path path = Paths.get("/Users/jeremy/source/confluent/kafka-connect/public/kafka-connect-cdc/kafka-connect-cdc-xstream/src/test/resources/io/confluent/kafka/connect/cdc/xstream/changes", fileName);

      if (!path.getParent().toFile().exists()) {
        path.getParent().toFile().mkdirs();
      }

      Queue<ChunkColumnValue> chunkColumnValues = new ArrayDeque<>(rowLCR.chunkColumnValues());
      XStreamOutput xStreamOutput = mock(XStreamOutput.class);
      when(xStreamOutput.receiveChunk()).thenAnswer(invocationOnMock -> chunkColumnValues.poll());
      when(xStreamOutput.connection()).thenReturn(this.connection);

      ChangeTestCase changeTestCase = new ChangeTestCase();
      changeTestCase.inputRowLCR = rowLCR;
      changeTestCase.inputTableMetadata = tableMetadataMap.get(objectName).expected();
      OracleChange oracleChange = OracleChange.build(xStreamOutput, changeTestCase.inputTableMetadata, rowLCR);
      changeTestCase.expected = JsonChange.convert(oracleChange);
      ChangeTestCase.write(path.toFile(), changeTestCase);
    }

  }

  //  @Disabled
  @TestFactory
  public Stream<DynamicTest> build() throws IOException {
    String packageName = this.getClass().getPackage().getName() + ".changes";
    List<ChangeTestCase> testCases = TestDataUtils.loadJsonResourceFiles(packageName, ChangeTestCase.class);
    return testCases.stream().map(data -> dynamicTest(data.name(), () -> build(data)));
  }

  private void build(ChangeTestCase testCase) throws StreamsException, SQLException {
    Queue<ChunkColumnValue> chunkColumnValues = new ArrayDeque<>(testCase.inputRowLCR().chunkColumnValues());
    XStreamOutput xStreamOutput = mock(XStreamOutput.class);
    when(xStreamOutput.receiveChunk()).thenAnswer(invocationOnMock -> chunkColumnValues.poll());
    when(xStreamOutput.connection()).thenReturn(this.connection);

    OracleChange actual = OracleChange.build(xStreamOutput, testCase.inputTableMetadata(), testCase.inputRowLCR());
    assertChange(testCase.expected(), actual);
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
  public static class ChangeTestCase implements NamedTest {
    @JsonIgnore
    String name;
    JsonRowLCR inputRowLCR;
    JsonTableMetadata inputTableMetadata;
    JsonChange expected;

    public static void write(File file, ChangeTestCase change) throws IOException {
      try (OutputStream outputStream = new FileOutputStream(file)) {
        ObjectMapperFactory.instance.writeValue(outputStream, change);
      }
    }

    public static void write(OutputStream outputStream, ChangeTestCase change) throws IOException {
      ObjectMapperFactory.instance.writeValue(outputStream, change);
    }

    public static ChangeTestCase read(InputStream inputStream) throws IOException {
      return ObjectMapperFactory.instance.readValue(inputStream, ChangeTestCase.class);
    }

    public static ChangeTestCase read(File inputFile) throws IOException {
      try (FileInputStream inputStream = new FileInputStream(inputFile)) {
        return read(inputStream);
      }
    }

    @Override
    public void name(String name) {
      this.name = name;
    }

    @Override
    public String name() {
      return this.name;
    }

    public JsonRowLCR inputRowLCR() {
      return this.inputRowLCR;
    }

    public void inputRowLCR(JsonRowLCR value) {
      this.inputRowLCR = value;
    }

    public JsonChange expected() {
      return this.expected;
    }

    public void expected(JsonChange value) {
      this.expected = value;
    }

    public JsonTableMetadata inputTableMetadata() {
      return this.inputTableMetadata;
    }

    public void inputTableMetadata(JsonTableMetadata value) {
      this.inputTableMetadata = value;
    }
  }


}
