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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class TypeConversion {
  private static final Logger log = LoggerFactory.getLogger(TypeConversion.class);
  final Map<SchemaKey, DatumConverter> converterLookup;

  public TypeConversion() {
    this.converterLookup = new HashMap<>();
    registerConverter(new Int8DatumConverter());
    registerConverter(new Int16DatumConverter());
    registerConverter(new Int32DatumConverter());
    registerConverter(new Int64DatumConverter());
    registerConverter(new DateDatumConverter());
    registerConverter(new DecimalDatumConverter());
    registerConverter(new TimestampDatumConverter());
    registerConverter(new Float32DatumConverter());
    registerConverter(new Float64DatumConverter());
    registerConverter(new StringDatumConverter());
    registerConverter(new BytesDatumConverter());
    registerConverter(new BooleanDatumConverter());
  }

  public final void registerConverter(DatumConverter converter) {
    Preconditions.checkNotNull(converter, "converter cannot be null.");

    SchemaKey schemaKey = new SchemaKey(converter.schema());

    if (this.converterLookup.containsKey(schemaKey)) {
      if (log.isWarnEnabled()) {
        log.warn("Schema {} is already registered to {}",
            schemaKey,
            this.converterLookup.get(schemaKey).getClass().getSimpleName()
        );
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("Registering schema {} to {}", schemaKey, converter);
    }

    this.converterLookup.put(schemaKey, converter);
  }

  public Object toConnect(Schema schema, Datum datum) throws SQLException {
    if (null == datum && !schema.isOptional()) {
      throw new DataException("schema is not optional and datum is null.");
    }

    if (null == datum) {
      return null;
    }
    SchemaKey schemaKey = new SchemaKey(schema);
    DatumConverter converter = this.converterLookup.get(schemaKey);

    if (null == converter) {
      throw new UnsupportedOperationException(
          String.format("Unsupported type: %s", schemaKey)
      );
    }

    return converter.toConnect(schema, datum);
  }

  public Datum toOracle(Schema schema, Object value) throws SQLException {
    if (null == value && !schema.isOptional()) {
      throw new DataException("schema is not optional and datum is null.");
    }

    if (null == value) {
      return null;
    }

    SchemaKey schemaKey = new SchemaKey(schema);
    DatumConverter converter = this.converterLookup.get(schemaKey);

    if (null == converter) {
      throw new UnsupportedOperationException(
          String.format("Unsupported type: %s", schemaKey)
      );
    }

    return converter.toOracle(schema, value);
  }

}
