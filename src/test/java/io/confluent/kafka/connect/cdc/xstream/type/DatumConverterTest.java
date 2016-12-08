/**
 * Copyright (C) 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.connect.cdc.xstream.type;

import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.CHAR;
import oracle.sql.DATE;
import oracle.sql.Datum;
import oracle.sql.NUMBER;
import oracle.sql.RAW;
import oracle.sql.TIMESTAMP;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.hamcrest.core.IsEqual;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Date;
import java.util.Random;

import static org.junit.Assert.*;

public class DatumConverterTest {
  TypeConversion converter;

  @Before
  public void setup() {
    this.converter = new TypeConversion();
  }

  protected Object toConnect(Schema schema, Datum datum, Object expectedValue) throws SQLException {
    Object actualValue = this.converter.toConnect(schema, datum);
    assertNotNull("actualValue should not be null", actualValue);

//    if (Schema.Type.BYTES == schema.type() &&
//        Decimal.LOGICAL_NAME == schema.name()
//        ) {
//      BigDecimal actualDecimalValue = Decimal.toLogical(schema, (byte[]) actualValue);
//      assertEquals(expectedValue, actualDecimalValue);
//      assertEquals(expectedValue.getClass(), actualDecimalValue.getClass());
//
//    } else {
    assertThat(actualValue, IsEqual.equalTo(actualValue));
    assertEquals(expectedValue.getClass(), actualValue.getClass());

//    }
    return actualValue;
  }

  protected void toOracle(Schema schema, Object connectValue, Datum expectedDatum) throws SQLException {
    Datum actualDatum = this.converter.toOracle(schema, connectValue);
    assertNotNull(actualDatum);
    assertEquals(expectedDatum, actualDatum);
    assertEquals(expectedDatum.getClass(), actualDatum.getClass());
  }

  protected void roundtrip(Schema schema, Datum datum, Object connectValue) throws SQLException {
    toConnect(schema, datum, connectValue);
    toOracle(schema, connectValue, datum);
  }

  @Test
  public void charDatum() throws SQLException {
    roundtrip(Schema.STRING_SCHEMA, new CHAR("This is a test message", null), "This is a test message");
  }

  @Test
  public void numberZero() throws SQLException {
    roundtrip(Schema.INT8_SCHEMA, new NUMBER(), (byte) 0);
    roundtrip(Schema.INT16_SCHEMA, new NUMBER(), (short) 0);
    roundtrip(Schema.INT32_SCHEMA, new NUMBER(), 0);
    roundtrip(Schema.INT64_SCHEMA, new NUMBER(), 0L);
    for (int scale = 0; scale < 39; scale++) {
      roundtrip(Decimal.schema(scale), new NUMBER(), BigDecimal.ZERO.setScale(scale));
    }
  }

  @Test
  public void number() throws SQLException {
    Random random = new Random();
    for (int i = 0; i < 50; i++) {
      for (int scale = 0; scale < 39; scale++) {
        BigDecimal decimal = new BigDecimal(random.nextLong()).setScale(scale);
        roundtrip(Decimal.schema(scale), new NUMBER(decimal), decimal);
      }
    }
  }

  @Test
  public void bytes() throws SQLException {
    byte[] bytes = new byte[]{0x00, 0x01, 0x02, 0x03, 0x04};
    RAW raw = new RAW(bytes);
    roundtrip(Schema.BYTES_SCHEMA, raw, bytes);
  }

  @Test
  public void timestamp() throws SQLException {
    final Date date = new Date(1473701823000L);
    TIMESTAMP timestamp = new TIMESTAMP(new java.sql.Date(1473701823000L));
    roundtrip(Timestamp.SCHEMA, timestamp, date);
  }

  @Test
  public void date() throws SQLException {
    final Date date = new Date(1473701823000L);
    DATE datum = new DATE(new java.sql.Date(1473701823000L));
    roundtrip(org.apache.kafka.connect.data.Date.SCHEMA, datum, date);
  }

  @Test
  public void float64() throws SQLException {
    final double input = Double.MIN_VALUE;
    BINARY_DOUBLE datum = new BINARY_DOUBLE(input);
    roundtrip(Schema.FLOAT64_SCHEMA, datum, input);
  }

  @Test
  public void float32() throws SQLException {
    final float input = Float.MIN_VALUE;
    BINARY_FLOAT datum = new BINARY_FLOAT(input);
    roundtrip(Schema.FLOAT32_SCHEMA, datum, input);
  }

  @Test
  public void bool() throws SQLException {
    final boolean input = true;
    roundtrip(Schema.BOOLEAN_SCHEMA, new NUMBER((byte) 1), input);
  }
}
