package io.confluent.kafka.connect.cdc.xstream.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.confluent.kafka.connect.cdc.JsonChange;
import io.confluent.kafka.connect.cdc.NamedTest;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ColumnValueTestCase extends TestCase implements NamedTest {
  JsonRowLCR.JsonColumnValue input;
  JsonChange.JsonColumnValue expected;
  @JsonIgnore
  String name;

  public JsonChange.JsonColumnValue expected() {
    return this.expected;
  }

  public void expected(JsonChange.JsonColumnValue value) {
    this.expected = value;
  }

  public JsonRowLCR.JsonColumnValue input() {
    return this.input;
  }

  public void input(JsonRowLCR.JsonColumnValue value) {
    this.input = value;
  }

  @Override
  public void name(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return this.name;
  }
}
