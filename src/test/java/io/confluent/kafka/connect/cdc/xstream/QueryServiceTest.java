package io.confluent.kafka.connect.cdc.xstream;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ServiceManager;
import io.confluent.kafka.connect.cdc.Change;
import io.confluent.kafka.connect.cdc.ChangeWriter;
import io.confluent.kafka.connect.cdc.Integration;
import io.confluent.kafka.connect.cdc.docker.DockerCompose;
import io.confluent.kafka.connect.cdc.xstream.docker.Oracle12cClusterHealthCheck;
import io.confluent.kafka.connect.cdc.xstream.docker.OracleSettings;
import io.confluent.kafka.connect.cdc.xstream.docker.OracleSettingsExtension;
import oracle.streams.StreamsException;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@Category(Integration.class)
@DockerCompose(dockerComposePath = Oracle12cTest.DOCKER_COMPOSE_FILE, clusterHealthCheck = Oracle12cClusterHealthCheck.class)
@ExtendWith(OracleSettingsExtension.class)
public class QueryServiceTest extends Oracle12cTest {
  private static final Logger log = LoggerFactory.getLogger(QueryServiceTest.class);
  XStreamSourceConnectorConfig config;
  OffsetStorageReader offsetStorageReader;
  QueryService queryService;
  ChangeWriter changeWriter;
  ServiceManager serviceManager;


  @BeforeEach
  public void setup(
      @OracleSettings Map<String, String> settings
  ) throws Exception {
    this.config = new XStreamSourceConnectorConfig(settings);
    this.offsetStorageReader = mock(OffsetStorageReader.class);
    this.changeWriter = mock(ChangeWriter.class);
    this.queryService = new QueryService(this.config, this.offsetStorageReader, this.changeWriter);
    this.serviceManager = new ServiceManager(Arrays.asList(this.queryService));
  }

  @Test
  public void receiveLCR() throws SQLException, StreamsException, TimeoutException {
    final List<Change> changes = new ArrayList<>();

    doAnswer(invocationOnMock -> {
      Change change = invocationOnMock.getArgumentAt(0, Change.class);
      changes.add(change);
      return null;
    }).when(this.changeWriter).addChange(any());

    this.serviceManager.startAsync();
    this.serviceManager.awaitHealthy(60, TimeUnit.SECONDS);

    Stopwatch stopwatch = Stopwatch.createStarted();

    while (changes.size() <= 30 || stopwatch.elapsed(TimeUnit.SECONDS) > 60L) {
      OracleChange oracleChange = this.queryService.receiveChange();
      if (log.isDebugEnabled()) {
        log.debug("oracleChange returned {}", oracleChange);
      }
      if (null != oracleChange) {
        changes.add(oracleChange);
      }
    }
  }

  @AfterEach
  public void after() throws Exception {
    this.serviceManager.stopAsync();
    this.serviceManager.awaitStopped(60, TimeUnit.SECONDS);
  }
}
