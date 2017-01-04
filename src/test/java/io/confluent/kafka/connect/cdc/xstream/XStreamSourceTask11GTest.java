package io.confluent.kafka.connect.cdc.xstream;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.cdc.docker.DockerFormatString;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Disabled
public class XStreamSourceTask11GTest extends Oracle11gTest {
  XStreamSourceTask xStreamSourceTask;

  @BeforeEach
  public void before(
      @DockerFormatString(container = XStreamConstants.ORACLE_CONTAINER, port = XStreamConstants.ORACLE_PORT, format = XStreamConstants.JDBC_URL_FORMAT_11G) String jdbcUrl
  ) {
    Map<String, String> settings = ImmutableMap.of(
        XStreamSourceConnectorConfig.JDBC_URL_CONF, jdbcUrl,
        XStreamSourceConnectorConfig.JDBC_USERNAME_CONF, XStreamConstants.XSTREAM_USERNAME_12C,
        XStreamSourceConnectorConfig.JDBC_PASSWORD_CONF, XStreamConstants.XSTREAM_PASSWORD_12C,
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

  @AfterEach
  public void stop() {
    this.xStreamSourceTask.stop();
  }


}
