package io.confluent.kafka.connect.cdc.xstream.schema;

import oracle.streams.ChunkColumnValue;
import oracle.streams.ColumnValue;
import oracle.streams.RowLCR;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class TableMetadataProviderTest {
  TableMetadataProvider tableMetadataProvider;
  String lastIntegerColumn;

  public static TableMetadataProvider mockTableMetadataProvider() throws SQLException {
    TableMetadataProvider tableMetadataProvider = mock(TableMetadataProvider.class, CALLS_REAL_METHODS);

    final List<Column> columns = new ArrayList<>();

    Column column = mock(Column.class);
    when(column.name()).thenReturn("ID");
    when(column.type()).thenReturn(ColumnValue.NUMBER);
    when(column.nullable()).thenReturn(true);
    when(column.comments()).thenReturn("This is the identifier of the row.");
    when(column.chunkColumn()).thenReturn(false);
    when(column.precision()).thenReturn(null);
    when(column.scale()).thenReturn(null);
    columns.add(column);

    column = mock(Column.class);
    when(column.name()).thenReturn("FIRST_NAME");
    when(column.type()).thenReturn(ColumnValue.CHAR);
    when(column.nullable()).thenReturn(true);
    when(column.comments()).thenReturn("The first name of the user.");
    when(column.chunkColumn()).thenReturn(false);
    when(column.precision()).thenReturn(null);
    when(column.scale()).thenReturn(null);
    columns.add(column);

    column = mock(Column.class);
    when(column.name()).thenReturn("LAST_NAME");
    when(column.type()).thenReturn(ColumnValue.CHAR);
    when(column.nullable()).thenReturn(true);
    when(column.comments()).thenReturn("The last name of the user.");
    when(column.chunkColumn()).thenReturn(false);
    when(column.precision()).thenReturn(null);
    when(column.scale()).thenReturn(null);
    columns.add(column);

    column = mock(Column.class);
    when(column.name()).thenReturn("EMAIL_ADDRESS");
    when(column.type()).thenReturn(ColumnValue.CHAR);
    when(column.nullable()).thenReturn(true);
    when(column.comments()).thenReturn("The email address of the user.");
    when(column.chunkColumn()).thenReturn(false);
    when(column.precision()).thenReturn(null);
    when(column.scale()).thenReturn(null);
    columns.add(column);

    column = mock(Column.class);
    when(column.name()).thenReturn("ADDRESS");
    when(column.type()).thenReturn(ColumnValue.CHAR);
    when(column.nullable()).thenReturn(true);
    when(column.comments()).thenReturn("The address of the user.");
    when(column.chunkColumn()).thenReturn(false);
    when(column.precision()).thenReturn(null);
    when(column.scale()).thenReturn(null);
    columns.add(column);

    column = mock(Column.class);
    when(column.name()).thenReturn("CITY");
    when(column.type()).thenReturn(ColumnValue.CHAR);
    when(column.nullable()).thenReturn(true);
    when(column.comments()).thenReturn("The city of the user.");
    when(column.chunkColumn()).thenReturn(false);
    when(column.precision()).thenReturn(null);
    when(column.scale()).thenReturn(null);
    columns.add(column);

    column = mock(Column.class);
    when(column.name()).thenReturn("STATE");
    when(column.type()).thenReturn(ColumnValue.CHAR);
    when(column.nullable()).thenReturn(true);
    when(column.comments()).thenReturn("The state of the user.");
    when(column.chunkColumn()).thenReturn(false);
    when(column.precision()).thenReturn(null);
    when(column.scale()).thenReturn(null);
    columns.add(column);

    when(tableMetadataProvider.getColumns(any(RowLCR.class))).thenReturn(columns);
    when(tableMetadataProvider.getKeys(any(RowLCR.class))).thenReturn(Arrays.asList("ID"));

    return tableMetadataProvider;
  }

  @Before
  public void setup() {
    this.tableMetadataProvider = mock(TableMetadataProvider.class, CALLS_REAL_METHODS);
  }

  void assertColumn(String dataType, final Integer precision, final Integer scale, final int expectedType, final boolean chunkColumnValue) throws SQLException {
    final ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getInt("COLUMN_ID")).thenReturn(1);
    when(resultSet.getString("COLUMN_NAME")).thenReturn("TEST_COLUMN");
    when(resultSet.getString("DATA_TYPE")).thenReturn(dataType);
    when(resultSet.getString("NULLABLE")).thenReturn("Y");
    when(resultSet.getString("COMMENTS")).thenReturn("This is a testing column.");

    when(resultSet.getInt("DATA_PRECISION")).then(new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
        lastIntegerColumn = invocationOnMock.getArgumentAt(0, String.class);
        return null == precision ? 0 : precision;
      }
    });
    when(resultSet.getInt("DATA_SCALE")).then(new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
        lastIntegerColumn = invocationOnMock.getArgumentAt(0, String.class);
        return null == scale ? 0 : scale;
      }
    });

    when(resultSet.wasNull()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
        boolean result;
        if (lastIntegerColumn.equalsIgnoreCase("DATA_SCALE")) {
          result = scale == null;
        } else if (lastIntegerColumn.equalsIgnoreCase("DATA_PRECISION")) {
          result = precision == null;
        } else {
          throw new IllegalStateException(lastIntegerColumn);
        }
        return result;
      }
    });

    final Column actual = this.tableMetadataProvider.column(resultSet);
    assertNotNull("actual should not be null.", actual);

    assertEquals("name does not match", resultSet.getString("COLUMN_NAME"), actual.name());
    assertEquals("nullable does not match", resultSet.getString("NULLABLE") == "Y", actual.nullable());
    assertEquals("type does not match", expectedType, actual.type());
    assertEquals("comments does not match", resultSet.getString("COMMENTS"), actual.comments());
    assertEquals("chunkColumn does not match", chunkColumnValue, actual.chunkColumn());
  }

  @Test
  public void binaryDouble() throws SQLException {
    assertColumn("BINARY_DOUBLE", null, null, ColumnValue.BINARY_DOUBLE, false);
  }

  @Test
  public void binaryFloat() throws SQLException {
    assertColumn("BINARY_FLOAT", null, null, ColumnValue.BINARY_FLOAT, false);
  }

  @Test
  public void blob() throws SQLException {
    assertColumn("BLOB", null, null, ChunkColumnValue.BLOB, true);
  }

  @Test
  public void Char() throws SQLException {
    assertColumn("CHAR", null, null, ColumnValue.CHAR, false);
  }

  @Test
  public void clob() throws SQLException {
    assertColumn("CLOB", null, null, ChunkColumnValue.CLOB, true);
  }

  @Test
  public void date() throws SQLException {
    assertColumn("DATE", null, null, ColumnValue.DATE, false);
  }

  @Test
  public void float_49() throws SQLException {
    assertColumn("FLOAT", 49, null, ColumnValue.NUMBER, false);
  }

  @Test
  public void float_126() throws SQLException {
    assertColumn("FLOAT", 126, null, ColumnValue.NUMBER, false);
  }

  @Test
  public void intervalDay0ToSecond0() throws SQLException {
    assertColumn("INTERVAL DAY(0) TO SECOND(0)", 0, 0, ColumnValue.INTERVALDS, false);
  }

  @Test
  public void intervalDay3ToSecond0() throws SQLException {
    assertColumn("INTERVAL DAY(3) TO SECOND(0)", 3, 0, ColumnValue.INTERVALDS, false);
  }

  @Test
  public void intervalDay3ToSecond2() throws SQLException {
    assertColumn("INTERVAL DAY(3) TO SECOND(2)", 3, 2, ColumnValue.INTERVALDS, false);
  }

  @Test
  public void intervalDay5ToSecond1() throws SQLException {
    assertColumn("INTERVAL DAY(5) TO SECOND(1)", 5, 1, ColumnValue.INTERVALDS, false);
  }

  @Test
  public void intervalDay9ToSecond6() throws SQLException {
    assertColumn("INTERVAL DAY(9) TO SECOND(6)", 9, 6, ColumnValue.INTERVALDS, false);
  }

  @Test
  public void intervalDay9ToSecond9() throws SQLException {
    assertColumn("INTERVAL DAY(9) TO SECOND(9)", 9, 9, ColumnValue.INTERVALDS, false);
  }

  @Test
  public void Long() throws SQLException {
    assertColumn("LONG", null, null, ChunkColumnValue.LONG, true);
  }

  @Test
  public void longRaw() throws SQLException {
    assertColumn("LONG RAW", null, null, ChunkColumnValue.LONGRAW, true);
  }

  @Test(expected = SQLException.class)
  public void mlsLabel() throws SQLException {
    assertColumn("MLSLABEL", null, null, -1, true);
  }

  @Test
  public void nchar() throws SQLException {
    assertColumn("NCHAR", null, null, ColumnValue.CHAR, false);
  }

  @Test
  public void nclob() throws SQLException {
    assertColumn("NCLOB", null, null, ChunkColumnValue.NCLOB, true);
  }

  @Test
  public void number_1_0() throws SQLException {
    assertColumn("NUMBER", 1, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_2_0() throws SQLException {
    assertColumn("NUMBER", 2, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_3_0() throws SQLException {
    assertColumn("NUMBER", 3, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_4_0() throws SQLException {
    assertColumn("NUMBER", 4, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_5_0() throws SQLException {
    assertColumn("NUMBER", 5, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_6_0() throws SQLException {
    assertColumn("NUMBER", 6, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_8_0() throws SQLException {
    assertColumn("NUMBER", 8, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_9_2() throws SQLException {
    assertColumn("NUMBER", 9, 2, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_10_0() throws SQLException {
    assertColumn("NUMBER", 10, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_11_0() throws SQLException {
    assertColumn("NUMBER", 11, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_11_8() throws SQLException {
    assertColumn("NUMBER", 11, 8, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_12_0() throws SQLException {
    assertColumn("NUMBER", 12, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_14_0() throws SQLException {
    assertColumn("NUMBER", 14, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_15_0() throws SQLException {
    assertColumn("NUMBER", 15, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_15_2() throws SQLException {
    assertColumn("NUMBER", 15, 2, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_16_0() throws SQLException {
    assertColumn("NUMBER", 16, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_20_0() throws SQLException {
    assertColumn("NUMBER", 20, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_22_0() throws SQLException {
    assertColumn("NUMBER", 22, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_28_0() throws SQLException {
    assertColumn("NUMBER", 28, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_38_0() throws SQLException {
    assertColumn("NUMBER", 38, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_null_0() throws SQLException {
    assertColumn("NUMBER", null, 0, ColumnValue.NUMBER, false);
  }

  @Test
  public void number_null_2() throws SQLException {
    assertColumn("NUMBER", null, 2, ColumnValue.NUMBER, false);
  }

  @Test
  public void number() throws SQLException {
    assertColumn("NUMBER", null, null, ColumnValue.NUMBER, false);
  }

  @Test
  public void nvarchar2() throws SQLException {
    assertColumn("NVARCHAR2", null, null, ColumnValue.CHAR, false);
  }

  @Test
  public void raw() throws SQLException {
    assertColumn("RAW", null, null, ColumnValue.RAW, false);
  }

  @Test(expected = SQLException.class)
  public void rowid() throws SQLException {
    assertColumn("ROWID", null, null, -1, false);
  }

  @Test
  public void timestamp_0() throws SQLException {
    assertColumn("TIMESTAMP(0)", null, 0, ColumnValue.TIMESTAMP, false);
  }

  @Test
  public void timestamp_3() throws SQLException {
    assertColumn("TIMESTAMP(3)", null, 3, ColumnValue.TIMESTAMP, false);
  }

  @Test
  public void timestamp_6() throws SQLException {
    assertColumn("TIMESTAMP(6)", null, 6, ColumnValue.TIMESTAMP, false);
  }

  @Test
  public void timestamp_9() throws SQLException {
    assertColumn("TIMESTAMP(9)", null, 9, ColumnValue.TIMESTAMP, false);
  }

  @Test
  public void timestamp_0_with_timezone() throws SQLException {
    assertColumn("TIMESTAMP(0) WITH TIME ZONE", null, 0, ColumnValue.TIMESTAMPTZ, false);
  }

  @Test
  public void timestamp_1_with_timezone() throws SQLException {
    assertColumn("TIMESTAMP(1) WITH TIME ZONE", null, 1, ColumnValue.TIMESTAMPTZ, false);
  }

  @Test
  public void timestamp_3_with_timezone() throws SQLException {
    assertColumn("TIMESTAMP(3) WITH TIME ZONE", null, 3, ColumnValue.TIMESTAMPTZ, false);
  }

  @Test
  public void timestamp_6_with_timezone() throws SQLException {
    assertColumn("TIMESTAMP(6) WITH TIME ZONE", null, 6, ColumnValue.TIMESTAMPTZ, false);
  }

  @Test
  public void timestamp_9_with_timezone() throws SQLException {
    assertColumn("TIMESTAMP(9) WITH TIME ZONE", null, 9, ColumnValue.TIMESTAMPTZ, false);
  }

  @Test
  public void timestamp_6_with_local_timezone() throws SQLException {
    assertColumn("TIMESTAMP(6) WITH LOCAL TIME ZONE", null, 6, ColumnValue.TIMESTAMPLTZ, false);
  }

  @Test(expected = SQLException.class)
  public void urowid() throws SQLException {
    assertColumn("UROWID", null, null, -1, false);
  }

  @Test(expected = SQLException.class)
  public void undefined() throws SQLException {
    assertColumn("UNDEFINED", null, null, -1, false);
  }

  @Test
  public void varchar2() throws SQLException {
    assertColumn("VARCHAR2", null, null, ColumnValue.CHAR, false);
  }

}
