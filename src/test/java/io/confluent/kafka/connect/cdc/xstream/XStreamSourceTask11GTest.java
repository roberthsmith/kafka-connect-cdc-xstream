package io.confluent.kafka.connect.cdc.xstream;

import org.junit.jupiter.api.Disabled;

@Disabled
public class XStreamSourceTask11GTest extends Oracle11gTest {
//  XStreamSourceTask xStreamSourceTask;
//
//  @BeforeEach
//  public void before(
//      @DockerFormatString(container = XStreamTestConstants.ORACLE_CONTAINER, port = XStreamTestConstants.ORACLE_PORT, format = XStreamTestConstants.JDBC_URL_FORMAT_11G) String jdbcUrl
//  ) {
//    Map<String, String> settings = ImmutableMap.of(
//        XStreamSourceConnectorConfig.JDBC_URL_CONF, jdbcUrl,
//        XStreamSourceConnectorConfig.JDBC_USERNAME_CONF, XStreamTestConstants.XSTREAM_USERNAME_12C,
//        XStreamSourceConnectorConfig.JDBC_PASSWORD_CONF, XStreamTestConstants.XSTREAM_PASSWORD_12C,
//        XStreamSourceConnectorConfig.XSTREAM_SERVER_NAMES_CONF, "xout"
//    );
//
//    this.xStreamSourceTask = new XStreamSourceTask();
//
//    SourceTaskContext context = mock(SourceTaskContext.class);
//    OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
//    when(offsetStorageReader.offset(anyMap())).thenReturn(new HashMap());
//    when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
//    this.xStreamSourceTask.initialize(context);
//    this.xStreamSourceTask.start(settings);
//  }
//
//  @Test
//  public void foo() {
//
//  }
//
//  @AfterEach
//  public void stop() {
//    this.xStreamSourceTask.stop();
//  }
//

}
