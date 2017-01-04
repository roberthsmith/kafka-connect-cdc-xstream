package io.confluent.kafka.connect.cdc.xstream;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.Integration;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import io.confluent.kafka.connect.cdc.TestDataUtils;
import io.confluent.kafka.connect.cdc.docker.DockerCompose;
import io.confluent.kafka.connect.cdc.docker.DockerFormatString;
import io.confluent.kafka.connect.cdc.xstream.docker.Oracle12cClusterHealthCheck;
import io.confluent.kafka.connect.cdc.xstream.model.TableMetadataTestCase;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.confluent.kafka.connect.cdc.ChangeAssertions.assertTableMetadata;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;

@Category(Integration.class)
@DockerCompose(dockerComposePath = Oracle12cTests.DOCKER_COMPOSE_FILE, clusterHealthCheck = Oracle12cClusterHealthCheck.class)
public class Oracle12cTableMetadataProviderTests extends Oracle12cTests {
  Oracle12cTableMetadataProvider tableMetadataProvider;
  XStreamSourceConnectorConfig config;
  OffsetStorageReader offsetStorageReader;


  @BeforeEach
  public void setup(
      @DockerFormatString(container = XStreamConstants.ORACLE_CONTAINER, port = XStreamConstants.ORACLE_PORT, format = XStreamConstants.JDBC_URL_FORMAT_12C_PDB) String jdbcUrl
  ) {
    Map<String, String> settings = ImmutableMap.of(
        XStreamSourceConnectorConfig.JDBC_URL_CONF, jdbcUrl,
        XStreamSourceConnectorConfig.JDBC_USERNAME_CONF, XStreamConstants.XSTREAM_USERNAME_12C,
        XStreamSourceConnectorConfig.JDBC_PASSWORD_CONF, XStreamConstants.XSTREAM_PASSWORD_12C,
        XStreamSourceConnectorConfig.XSTREAM_SERVER_NAMES_CONF, "xout"
    );

    this.config = new XStreamSourceConnectorConfig(settings);
    this.offsetStorageReader = mock(OffsetStorageReader.class);
    this.tableMetadataProvider = new Oracle12cTableMetadataProvider(this.config, this.offsetStorageReader);
  }


  @TestFactory
  public Stream<DynamicTest> fetchTableMetadata() throws IOException {
    String packageName = this.getClass().getPackage().getName() + ".tablemetadata";
    List<TableMetadataTestCase> testCases = TestDataUtils.loadJsonResourceFiles(packageName, TableMetadataTestCase.class);
    return testCases.stream().map(data -> dynamicTest(data.name(), () -> fetchTableMetadata(data)));
  }

  private void fetchTableMetadata(TableMetadataTestCase testCase) throws SQLException {
    assertNotNull(testCase, "testcase should not be null.");
    assertNotNull(testCase.databaseName(), "testcase.databaseName() should not be null.");
    assertNotNull(testCase.schemaName(), "testcase.schemaName() should not be null.");
    assertNotNull(testCase.tableName(), "testcase.tableName() should not be null.");
    assertNotNull(testCase.expected(), "testcase.expected() should not be null.");

    ChangeKey changeKey = new ChangeKey(testCase.databaseName(), testCase.schemaName(), testCase.tableName());
    TableMetadataProvider.TableMetadata tableMetadata = this.tableMetadataProvider.fetchTableMetadata(changeKey);
    assertNotNull(tableMetadata, "tableMetadata should not be null.");
    assertTableMetadata(testCase.expected(), tableMetadata);
  }
}
