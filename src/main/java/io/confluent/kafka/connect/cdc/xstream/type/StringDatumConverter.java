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

import oracle.sql.CHAR;
import oracle.sql.Datum;
import org.apache.kafka.connect.data.Schema;

import java.sql.SQLException;

class StringDatumConverter implements DatumConverter {
  @Override
  public Schema schema() {
    return Schema.STRING_SCHEMA;
  }

  @Override
  public Object toConnect(Schema schema, Datum datum) throws SQLException {
    return datum.stringValue();
  }

  @Override
  public Datum toOracle(Schema schema, Object value) throws SQLException {
    return new CHAR((String) value, null);
  }
}
