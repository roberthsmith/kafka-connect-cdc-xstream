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

import com.google.common.base.Preconditions;
import oracle.sql.Datum;
import oracle.sql.NUMBER;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;

import java.math.BigDecimal;
import java.sql.SQLException;

class DecimalDatumConverter implements DatumConverter {
  @Override
  public Schema schema() {
    return Decimal.schema(1);
  }

  @Override
  public Object toConnect(Schema schema, Datum datum) throws SQLException {
    String scaleString = schema.parameters().get(Decimal.SCALE_FIELD);
    BigDecimal decimal = datum.bigDecimalValue();
    final Integer scale = Integer.parseInt(scaleString);
    Preconditions.checkState(scale >= 0, "Invalid scale(%s). Scale must be greater than -1.", scale);
    if (scale != decimal.scale()) {
      decimal = decimal.setScale(scale);
    }
    return decimal;
  }

  @Override
  public Datum toOracle(Schema schema, Object value) throws SQLException {
    return new NUMBER((BigDecimal) value);
  }
}
