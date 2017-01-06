package io.confluent.kafka.connect.cdc.xstream.model;

import io.confluent.kafka.connect.cdc.ObjectMapperFactory;
import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.CHAR;
import oracle.sql.CharacterSet;
import oracle.sql.Datum;
import oracle.sql.NUMBER;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class JsonDatumTests {

  @BeforeAll
  public static void init() {
    TestCase.init();
  }

  void assertDatum(final Datum expected) throws SQLException, IOException {
    byte[] buffer = ObjectMapperFactory.instance.writeValueAsBytes(expected);
    final Datum actual = ObjectMapperFactory.instance.readValue(buffer, Datum.class);
    assertEquals(expected, actual);
  }

  @TestFactory
  public Stream<DynamicTest> roundtrip() throws IOException, SQLException {
    List<Datum> testCases = Arrays.asList(
        new NUMBER(BigDecimal.valueOf(314, 2)),
        new CHAR("This is a test value", CharacterSet.make(CharacterSet.DEFAULT_CHARSET)),
        new BINARY_DOUBLE(3.14D),
        new BINARY_FLOAT(3.14F)
    );

    return testCases.stream().map(testcase -> dynamicTest(testcase.getClass().getSimpleName(), () -> assertDatum(testcase)));
  }
}
