package io.confluent.kafka.connect.cdc.xstream;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.Before;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class XStreamSourceTask12cTest extends Oracle12cTest {
  XStreamSourceTask xStreamSourceTask;

  @Before
  public void before() {
    Map<String, String> settings = ImmutableMap.of(
        XStreamSourceConnectorConfig.JDBC_URL_CONF, jdbcUrl,
        XStreamSourceConnectorConfig.JDBC_USERNAME_CONF, DockerUtils.XSTREAM_USERNAME,
        XStreamSourceConnectorConfig.JDBC_PASSWORD_CONF, DockerUtils.XSTREAM_PASSWORD,
        XStreamSourceConnectorConfig.XSTREAM_SERVER_NAMES_CONF, "xout"
    );

    this.xStreamSourceTask = new XStreamSourceTask();

    SourceTaskContext context = mock(SourceTaskContext.class);
    OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
    when(offsetStorageReader.offset(anyMap())).thenReturn(new HashMap());
    when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
    this.xStreamSourceTask.initialize(context);
    this.xStreamSourceTask.start(settings);
  }

  @Test
  public void foo() {

  }


  @AfterAll
  public void stop() {
    this.xStreamSourceTask.stop();
  }

  @AfterEach
  public static void afterClass() {
    docker.after();
  }


}
