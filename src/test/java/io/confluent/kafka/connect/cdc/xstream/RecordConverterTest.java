package io.confluent.kafka.connect.cdc.xstream;

import io.confluent.kafka.connect.cdc.xstream.schema.TableMetadataProvider;
import io.confluent.kafka.connect.cdc.xstream.schema.TableMetadataProviderTest;
import oracle.sql.CHAR;
import oracle.sql.DATE;
import oracle.sql.Datum;
import oracle.sql.NUMBER;
import oracle.streams.ChunkColumnValue;
import oracle.streams.ColumnValue;
import oracle.streams.RowLCR;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RecordConverterTest {

  Deque<ChunkColumnValue> chunks(LinkedHashMap<String, Datum> columnValues) {
    Deque<ChunkColumnValue> deque = new ArrayDeque<>();

    int index = 0;
    for (Map.Entry<String, Datum> kvp : columnValues.entrySet()) {
      index++;
      ChunkColumnValue chunkColumnValue = mock(ChunkColumnValue.class);
      when(chunkColumnValue.isEndOfRow()).thenReturn(index == columnValues.size());
      when(chunkColumnValue.getColumnName()).thenReturn(kvp.getKey());
      when(chunkColumnValue.getColumnData()).thenReturn(kvp.getValue());
      deque.add(chunkColumnValue);
    }

    return deque;
  }

  @Test
  public void convert() throws Exception {
    Callable<ChunkColumnValue> chunkColumnValueCallable = mock(Callable.class);
    LinkedHashMap<String, Datum> columnValues = new LinkedHashMap<>();
    columnValues.put("ID", new NUMBER(new BigDecimal(1234).setScale(0)));
    columnValues.put("FIRST_NAME", new CHAR("Freddy", null));
    columnValues.put("LAST_NAME", new CHAR("User", null));
    columnValues.put("EMAIL_ADDRESS", new CHAR("freddy.user@example.com", null));
    columnValues.put("ADDRESS", new CHAR("123 Main St", null));
    columnValues.put("CITY", new CHAR("Austin", null));
    columnValues.put("STATE", new CHAR("TX", null));
    final Deque<ChunkColumnValue> chunks = chunks(columnValues);

    when(chunkColumnValueCallable.call()).thenAnswer(new Answer<ChunkColumnValue>() {
      @Override
      public ChunkColumnValue answer(InvocationOnMock invocationOnMock) throws Throwable {
        return chunks.poll();
      }
    });

    XStreamSourceConnectorConfig config = new XStreamSourceConnectorConfig(XStreamSourceConnectorConfigTest.settings());
    Connection connection = ConnectionMock.mockConnection();

    TableMetadataProvider tableMetadataProvider = TableMetadataProviderTest.mockTableMetadataProvider();
    SchemaGenerator schemaGenerator = new SchemaGenerator(config, connection, tableMetadataProvider);

    RecordConverter converter = new RecordConverter(chunkColumnValueCallable, schemaGenerator, config, "xout");

    final Date sourceTime = new Date(1474774749000L);

    RowLCR rowLCR = mock(RowLCR.class);
    when(rowLCR.hasChunkData()).thenReturn(false);
    when(rowLCR.getSourceDatabaseName()).thenReturn("SourceDatabase");
    when(rowLCR.getObjectName()).thenReturn("ObjectName");
    when(rowLCR.getObjectOwner()).thenReturn("ObjectOwner");
    when(rowLCR.getPosition()).thenReturn(NUMBER.toBytes(1));
    when(rowLCR.getSourceTime()).thenReturn(new DATE(new java.sql.Date(sourceTime.getTime())));


    ColumnValue[] values = new ColumnValue[columnValues.size()];
    int i = 0;
    for (Map.Entry<String, Datum> kvp : columnValues.entrySet()) {
      ColumnValue columnValue = mock(ColumnValue.class);
      when(columnValue.getColumnData()).thenReturn(kvp.getValue());
      when(columnValue.getColumnName()).thenReturn(kvp.getKey());
      values[i++] = columnValue;
    }

    when(rowLCR.getNewValues()).thenReturn(values);

    SourceRecord sourceRecord = converter.convert(rowLCR);
    assertNotNull(sourceRecord);
    assertNotNull("key should not be null.", sourceRecord.key());
    assertNotNull("value should not be null.", sourceRecord.value());
    assertTrue("key should be a struct.", sourceRecord.key() instanceof Struct);
    assertTrue("value should be a struct.", sourceRecord.value() instanceof Struct);
    assertEquals("topic does not match.", "SourceDatabase.ObjectName", sourceRecord.topic());

  }

}
