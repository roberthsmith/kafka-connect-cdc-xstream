package io.confluent.kafka.connect.cdc.xstream;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.Integration;
import io.confluent.kafka.connect.cdc.JdbcUtils;
import io.confluent.kafka.connect.cdc.JsonChange;
import io.confluent.kafka.connect.cdc.JsonTableMetadata;
import io.confluent.kafka.connect.cdc.NamedTest;
import io.confluent.kafka.connect.cdc.ObjectMapperFactory;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import io.confluent.kafka.connect.cdc.TestDataUtils;
import io.confluent.kafka.connect.cdc.docker.DockerCompose;
import io.confluent.kafka.connect.cdc.xstream.docker.Oracle12cClusterHealthCheck;
import io.confluent.kafka.connect.cdc.xstream.docker.Oracle12cSettings;
import io.confluent.kafka.connect.cdc.xstream.docker.SettingsExtension;
import io.confluent.kafka.connect.cdc.xstream.model.JsonRowLCR;
import oracle.streams.ChunkColumnValue;
import oracle.streams.StreamsException;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.PooledConnection;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Stream;

import static io.confluent.kafka.connect.cdc.ChangeAssertions.assertChange;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(Integration.class)
@DockerCompose(dockerComposePath = Oracle12cTest.DOCKER_COMPOSE_FILE, clusterHealthCheck = Oracle12cClusterHealthCheck.class)
@ExtendWith(SettingsExtension.class)
public class OracleChangeTest extends Oracle12cTest {
  private static final Logger log = LoggerFactory.getLogger(OracleChangeTest.class);

  PooledConnection connection;
  XStreamSourceConnectorConfig config;

  @BeforeEach
  public void config(
      @Oracle12cSettings
          Map<String, String> settings
  ) throws SQLException {
    this.config = new XStreamSourceConnectorConfig(settings);
    this.connection = JdbcUtils.openPooledConnection(this.config, new ChangeKey(XStreamTestConstants.ORACLE_PDB_DATABASE, null, null));
  }

  @AfterEach
  public void cleanup() throws SQLException {
    JdbcUtils.closeConnection(this.connection);
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

    TableMetadataProvider tableMetadataProvider = mock(TableMetadataProvider.class);
    when(tableMetadataProvider.tableMetadata(any(ChangeKey.class))).thenReturn(testCase.inputTableMetadata());

    OracleChange.Builder builder = new OracleChange.Builder(this.config, xStreamOutput, tableMetadataProvider);
    OracleChange actual = builder.build(testCase.inputRowLCR());
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
