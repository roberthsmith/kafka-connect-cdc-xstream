package io.confluent.kafka.connect.cdc.xstream;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConnectionMock {
  static ResultSet mockResultset(List<Object[]> rows, String... columnNames) throws SQLException {
    final Deque<Object[]> deque = new ArrayDeque<>(rows);
    ResultSet resultSet = mock(ResultSet.class);
    final AtomicReference<Object[]> currentRow = new AtomicReference<>();
    final AtomicInteger lastIndex = new AtomicInteger();
    when(resultSet.next()).then(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
        if (deque.isEmpty()) {
          return false;
        }

        Object[] c = deque.poll();
        currentRow.set(c);
        return true;
      }
    });
    for (int i = 0; i < columnNames.length; i++) {
      String columnName = columnNames[i];
      final int index = i;
      when(resultSet.getString(columnName)).thenAnswer(new Answer<String>() {
        @Override
        public String answer(InvocationOnMock invocationOnMock) throws Throwable {
          Object[] c = currentRow.get();
          return (String) c[index];
        }
      });
      when(resultSet.getString(index)).thenAnswer(new Answer<String>() {
        @Override
        public String answer(InvocationOnMock invocationOnMock) throws Throwable {
          Object[] c = currentRow.get();
          return (String) c[index];
        }
      });
      when(resultSet.getInt(columnName)).thenAnswer(new Answer<Integer>() {
        @Override
        public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
          lastIndex.set(index);
          Object[] c = currentRow.get();
          return (Integer) c[index];
        }
      });
      when(resultSet.getInt(index)).thenAnswer(new Answer<Integer>() {
        @Override
        public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
          lastIndex.set(index);
          Object[] c = currentRow.get();
          return (Integer) c[index];
        }
      });
      when(resultSet.getShort(columnName)).thenAnswer(new Answer<Short>() {
        @Override
        public Short answer(InvocationOnMock invocationOnMock) throws Throwable {
          lastIndex.set(index);
          Object[] c = currentRow.get();
          return (Short) c[index];
        }
      });
      when(resultSet.getShort(index)).thenAnswer(new Answer<Short>() {
        @Override
        public Short answer(InvocationOnMock invocationOnMock) throws Throwable {
          lastIndex.set(index);
          Object[] c = currentRow.get();
          return (Short) c[index];
        }
      });
    }

    when(resultSet.wasNull()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
        int index = lastIndex.get();
        Object[] c = currentRow.get();
        return null == c[index];
      }
    });


    return resultSet;
  }

  static ResultSet getColumnsResult() throws IOException, SQLException {
    List<String[]> allRows;
    try (InputStream inputStream = SchemaGeneratorTest.class.getResourceAsStream("columns.csv")) {
      try (Reader inputStreamReader = new InputStreamReader(inputStream)) {
        CSVParserBuilder builder = new CSVParserBuilder().withFieldAsNull(CSVReaderNullFieldIndicator.BOTH);
        try (CSVReader csvReader = new CSVReader(inputStreamReader, 0, builder.build())) {
          allRows = csvReader.readAll();
        }
      }
    }

    String[] headers = allRows.remove(0);
    List<Object[]> values = new ArrayList<>(allRows.size());

    for (String[] row : allRows) {
      Object[] newRow = new Object[headers.length];
      for (int i = 0; i < newRow.length; i++) {
        String columnName = headers[i];

        if (null == row[i]) {
          newRow[i] = null;
          continue;
        }

        switch (columnName) {
          case "CHAR_OCTET_LENGTH":
          case "COLUMN_SIZE":
          case "DATA_TYPE":
          case "DECIMAL_DIGITS":
          case "NULLABLE":
          case "NUM_PREC_RADIX":
          case "ORDINAL_POSITION":
          case "SQL_DATA_TYPE":
          case "SQL_DATETIME_SUB":
            newRow[i] = Integer.parseInt(row[i]);
            break;
          case "SOURCE_DATA_TYPE":
            newRow[i] = Short.parseShort(row[i]);
            break;
          default:
            newRow[i] = row[i];
            break;
        }
      }
      values.add(newRow);
    }

    return mockResultset(values, headers);
  }

  static ResultSet getKeyResultSet() throws IOException, SQLException {
    List<String[]> allRows;
    try (InputStream inputStream = SchemaGeneratorTest.class.getResourceAsStream("keys.csv")) {
      try (Reader inputStreamReader = new InputStreamReader(inputStream)) {
        CSVParserBuilder builder = new CSVParserBuilder().withFieldAsNull(CSVReaderNullFieldIndicator.BOTH);
        try (CSVReader csvReader = new CSVReader(inputStreamReader, 0, builder.build())) {
          allRows = csvReader.readAll();
        }
      }
    }

    String[] headers = allRows.remove(0);
    List<Object[]> values = new ArrayList<>(allRows.size());

    for (String[] row : allRows) {
      Object[] newRow = new Object[headers.length];
      for (int i = 0; i < newRow.length; i++) {
        String columnName = headers[i];

        if (null == row[i]) {
          newRow[i] = null;
          continue;
        }

        switch (columnName) {
          case "SCOPE":
          case "COLUMN_SIZE":
          case "DATA_TYPE":
          case "DECIMAL_DIGITS":
          case "BUFFER_LENGTH":
          case "NUM_PREC_RADIX":
          case "ORDINAL_POSITION":
          case "SQL_DATA_TYPE":
          case "SQL_DATETIME_SUB":
            newRow[i] = Integer.parseInt(row[i]);
            break;
          case "SOURCE_DATA_TYPE":
            newRow[i] = Short.parseShort(row[i]);
            break;
          default:
            newRow[i] = row[i];
            break;
        }
      }
      values.add(newRow);
    }

    return mockResultset(values, headers);
  }

  public static Connection mockConnection() throws SQLException, IOException {
    return mockConnection(12);
  }

  public static Connection mockConnection(int version) throws SQLException, IOException {
    Connection connection = mock(Connection.class);
    DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
    when(databaseMetaData.getDatabaseMajorVersion()).thenReturn(version);
    when(connection.getMetaData()).thenReturn(databaseMetaData);


    ResultSet columnsResultSet = getColumnsResult();
    when(databaseMetaData.getColumns(anyString(), anyString(), anyString(), anyString())).thenReturn(columnsResultSet);

    ResultSet keysResultSet = getKeyResultSet();
    when(databaseMetaData.getBestRowIdentifier(anyString(), anyString(), anyString(), anyInt(), anyBoolean())).thenReturn(keysResultSet);

    return connection;
  }


}
