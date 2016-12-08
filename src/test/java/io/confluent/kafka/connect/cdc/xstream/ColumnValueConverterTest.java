package io.confluent.kafka.connect.cdc.xstream;

import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.CHAR;
import oracle.sql.DATE;
import oracle.sql.Datum;
import oracle.sql.RAW;
import oracle.sql.TIMESTAMP;
import oracle.streams.ChunkColumnValue;
import oracle.streams.ColumnValue;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.hamcrest.core.IsEqual;
import org.junit.Before;
import org.junit.Test;

import java.security.SecureRandom;
import java.sql.Blob;
import java.sql.Date;
import java.sql.SQLException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ColumnValueConverterTest {
  ColumnValueConverter columnValueConverter;
  Random random;

  @Before
  public void before() {
    this.columnValueConverter = new ColumnValueConverter();
    this.random = new SecureRandom();
  }

  @Test
  public void convertColumnValue_CHAR() throws SQLException {
    final Datum columnData = new CHAR("foo", CHAR.DEFAULT_CHARSET);
    final int columnDataType = ColumnValue.CHAR;
    final Object expectedValue = "foo";
    final Schema expectedSchema = Schema.OPTIONAL_STRING_SCHEMA;

    convertColumnValue(columnDataType, expectedValue, expectedSchema, columnData);
  }

  @Test
  public void convertColumnValue_DATE() throws SQLException {
    final long expected = 1481172283000L;
    final Datum columnData = new DATE(new Date(expected));
    final int columnDataType = ColumnValue.DATE;
    final Object expectedValue = new Date(expected);
    final Schema expectedSchema = org.apache.kafka.connect.data.Date.builder().optional().build();

    convertColumnValue(columnDataType, expectedValue, expectedSchema, columnData);
  }

  @Test
  public void convertColumnValue_RAW() throws SQLException {
    final byte[] expected = new byte[]{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x10};
    final Datum columnData = new RAW(expected);
    final int columnDataType = ColumnValue.RAW;
    final Object expectedValue = expected;
    final Schema expectedSchema = Schema.OPTIONAL_BYTES_SCHEMA;

    convertColumnValue(columnDataType, expectedValue, expectedSchema, columnData);
  }

  @Test
  public void convertColumnValue_TIMESTAMP() throws SQLException {
    final long expected = 1481172283000L;
    final Datum columnData = new TIMESTAMP(new Date(expected));
    final int columnDataType = ColumnValue.TIMESTAMP;
    final Object expectedValue = new Date(expected);
    final Schema expectedSchema = Timestamp.builder().optional().build();

    convertColumnValue(columnDataType, expectedValue, expectedSchema, columnData);
  }

  @Test
  public void convertColumnValue_BINARY_DOUBLE() throws SQLException {
    final double expected = 3.14D;
    final Datum columnData = new BINARY_DOUBLE(expected);
    final int columnDataType = ColumnValue.BINARY_DOUBLE;
    final Object expectedValue = expected;
    final Schema expectedSchema = Schema.OPTIONAL_FLOAT64_SCHEMA;

    convertColumnValue(columnDataType, expectedValue, expectedSchema, columnData);
  }

  @Test
  public void convertColumnValue_BINARY_FLOAT() throws SQLException {
    final float expected = 3.14F;
    final Datum columnData = new BINARY_FLOAT(expected);
    final int columnDataType = ColumnValue.BINARY_FLOAT;
    final Object expectedValue = expected;
    final Schema expectedSchema = Schema.OPTIONAL_FLOAT32_SCHEMA;

    convertColumnValue(columnDataType, expectedValue, expectedSchema, columnData);
  }

  private void convertColumnValue(int columnDataType, Object expectedValue, Schema expectedSchema, Datum columnData) throws SQLException {
    final String columnName = "TestColumnName";
    ColumnValue columnValue = mock(ColumnValue.class);
    when(columnValue.getColumnData()).thenReturn(columnData);
    when(columnValue.getColumnDataType()).thenReturn(columnDataType);
    when(columnValue.getColumnName()).thenReturn(columnName);
    OracleColumnValue result = this.columnValueConverter.convertColumnValue(columnValue);
    assertNotNull("result should not be null.", result);
    assertEquals("columnName does not match.", columnName, result.columnName);
    assertEquals("schema does not match.", expectedSchema, result.schema);
    assertThat("value does not match.", result.value, IsEqual.equalTo(expectedValue));
  }


}
