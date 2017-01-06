package io.confluent.kafka.connect.cdc.xstream.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import io.confluent.kafka.connect.cdc.NamedTest;
import io.confluent.kafka.connect.cdc.ObjectMapperFactory;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class TableMetadataTestCase extends TestCase implements NamedTest {
  String databaseName;
  String schemaName;
  String tableName;
  TableMetadataProvider.TableMetadata expected;

  public static void write(File file, TableMetadataTestCase change) throws IOException {
    try (OutputStream outputStream = new FileOutputStream(file)) {
      ObjectMapperFactory.instance.writeValue(outputStream, change);
    }
  }

  public static void write(OutputStream outputStream, TableMetadataTestCase change) throws IOException {
    ObjectMapperFactory.instance.writeValue(outputStream, change);
  }

  public static TableMetadataTestCase read(InputStream inputStream) throws IOException {
    return ObjectMapperFactory.instance.readValue(inputStream, TableMetadataTestCase.class);
  }

  public String databaseName() {
    return this.databaseName;
  }

  public void databaseName(String value) {
    this.databaseName = value;
  }

  public String schemaName() {
    return this.schemaName;
  }

  public void schemaName(String value) {
    this.schemaName = value;
  }

  public String tableName() {
    return this.tableName;
  }

  public void tableName(String value) {
    this.tableName = value;
  }

  public TableMetadataProvider.TableMetadata expected() {
    return this.expected;
  }

  public void expected(TableMetadataProvider.TableMetadata value) {
    this.expected = value;
  }

  @Override
  public void name(String name) {

  }

  @Override
  public String name() {
    return this.tableName();
  }
}
