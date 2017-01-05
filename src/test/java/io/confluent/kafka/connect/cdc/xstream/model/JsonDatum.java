package io.confluent.kafka.connect.cdc.xstream.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.CHAR;
import oracle.sql.CharacterSet;
import oracle.sql.DATE;
import oracle.sql.Datum;
import oracle.sql.NUMBER;
import oracle.sql.RAW;
import oracle.sql.TIMESTAMP;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;
import oracle.streams.ColumnValue;

import java.io.IOException;

class JsonDatum {
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class Storage {
    @JsonProperty
    int charset;

    @JsonProperty
    int datumType;

    @JsonProperty
    byte[] value;
  }

  static class DatumSerializer extends JsonSerializer<Datum> {
    @Override
    public void serialize(Datum datum, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
      Storage storage = new Storage();
      storage.value = datum.getBytes();

      if (datum instanceof DATE) {
        storage.datumType = ColumnValue.DATE;
      } else if (datum instanceof CHAR) {
        storage.charset = ((CHAR) datum).getCharacterSet().getOracleId();
        storage.datumType = ColumnValue.CHAR;
      } else if (datum instanceof NUMBER) {
        storage.datumType = ColumnValue.NUMBER;
      } else if (datum instanceof TIMESTAMP) {
        storage.datumType = ColumnValue.TIMESTAMP;
      } else if (datum instanceof BINARY_DOUBLE) {
        storage.datumType = ColumnValue.BINARY_DOUBLE;
      } else if (datum instanceof BINARY_FLOAT) {
        storage.datumType = ColumnValue.BINARY_FLOAT;
      } else if (datum instanceof TIMESTAMPLTZ) {
        storage.datumType = ColumnValue.TIMESTAMPLTZ;
      } else if (datum instanceof TIMESTAMPTZ) {
        storage.datumType = ColumnValue.TIMESTAMPTZ;
      } else {
        throw new UnsupportedOperationException("datum not supported. " + datum.toString());
      }

      jsonGenerator.writeObject(storage);
    }
  }

  static class DatumDeserializer extends JsonDeserializer<Datum> {

    @Override
    public Datum deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
      Storage storage = jsonParser.readValueAs(Storage.class);

      Datum datum;

      switch (storage.datumType) {
        case ColumnValue.CHAR:
          datum = new CHAR(storage.value, CharacterSet.make(storage.charset));
          break;
        case ColumnValue.BINARY_DOUBLE:
          datum = new BINARY_DOUBLE(storage.value);
          break;
        case ColumnValue.BINARY_FLOAT:
          datum = new BINARY_FLOAT(storage.value);
          break;
        case ColumnValue.DATE:
          datum = new DATE(storage.value);
          break;
        case ColumnValue.NUMBER:
          datum = new NUMBER(storage.value);
          break;
        case ColumnValue.RAW:
          datum = new RAW(storage.value);
          break;
        case ColumnValue.TIMESTAMP:
          datum = new TIMESTAMP(storage.value);
          break;
        case ColumnValue.TIMESTAMPLTZ:
          datum = new TIMESTAMPLTZ(storage.value);
          break;
        case ColumnValue.TIMESTAMPTZ:
          datum = new TIMESTAMPTZ(storage.value);
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Could not deserialize type %s", storage.datumType)
          );
      }

      return datum;
    }


  }
}
