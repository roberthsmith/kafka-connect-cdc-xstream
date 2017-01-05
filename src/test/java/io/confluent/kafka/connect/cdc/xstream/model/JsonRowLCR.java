package io.confluent.kafka.connect.cdc.xstream.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import io.confluent.kafka.connect.cdc.NamedTest;
import io.confluent.kafka.connect.cdc.ObjectMapperFactory;
import io.confluent.kafka.connect.cdc.xstream.XStreamOutput;
import oracle.sql.DATE;
import oracle.sql.Datum;
import oracle.streams.ChunkColumnValue;
import oracle.streams.ColumnValue;
import oracle.streams.LCR;
import oracle.streams.RowLCR;
import oracle.streams.StreamsException;
import org.apache.kafka.connect.errors.DataException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JsonRowLCR extends TestCase implements RowLCR, NamedTest {
  @JsonIgnore
  String name;
  boolean hasChunkData;
  String sourceDatabaseName;
  String commandType;
  String objectOwner;
  String objectName;
  byte[] tag;
  byte[] position;
  String transactionID;
  byte[] sourceTime;
  Map<Object, Datum> attributes = new LinkedHashMap<>();
  JsonColumnValue[] oldValues = new JsonColumnValue[0];
  JsonColumnValue[] newValues = new JsonColumnValue[0];
  List<JsonChunkColumnValue> chunkColumnValues = new ArrayList<>();

  private static JsonColumnValue[] convertColumnValues(ColumnValue[] columnValues) throws StreamsException {
    int length = columnValues == null ? 0 : columnValues.length;
    JsonColumnValue[] convertedValues = new JsonColumnValue[length];
    for (int i = 0; i < columnValues.length; i++) {
      ColumnValue inputValue = columnValues[i];
      JsonColumnValue outputValue;
      if (inputValue instanceof JsonColumnValue) {
        outputValue = (JsonColumnValue) inputValue;
      } else {
        outputValue = JsonColumnValue.build(inputValue);
      }
      convertedValues[i] = outputValue;
    }
    return convertedValues;
  }

  static void copyAttributes(LCR sourceLCR, LCR targetLCR) {
    copyAttribute(LCR.ATTRIBUTE_ROOT_NAME, sourceLCR, targetLCR);
    copyAttribute(LCR.ATTRIBUTE_TX_NAME, sourceLCR, targetLCR);
    copyAttribute(LCR.ATTRIBUTE_ROW_ID, sourceLCR, targetLCR);
    copyAttribute(LCR.ATTRIBUTE_SERIAL_NUM, sourceLCR, targetLCR);
    copyAttribute(LCR.ATTRIBUTE_SESSION_NUM, sourceLCR, targetLCR);
    copyAttribute(LCR.ATTRIBUTE_THREAD_NUM, sourceLCR, targetLCR);
    copyAttribute(LCR.ATTRIBUTE_USERNAME, sourceLCR, targetLCR);
  }

  static void copyAttribute(String attributeName, LCR sourceLCR, LCR targetLCR) {
    Object value = sourceLCR.getAttribute(attributeName);

    if (null != value) {
      targetLCR.setAttribute(attributeName, value);
    }
  }

  public static JsonRowLCR build(XStreamOutput xStreamOutput, RowLCR rowLCR) throws StreamsException {
    JsonRowLCR jsonRowLCR = new JsonRowLCR();
    jsonRowLCR.setSourceTime(rowLCR.getSourceTime());
    jsonRowLCR.setChunkDataFlag(rowLCR.hasChunkData());
    jsonRowLCR.setCommandType(rowLCR.getCommandType());
    jsonRowLCR.setObjectName(rowLCR.getObjectName());
    jsonRowLCR.setObjectOwner(rowLCR.getObjectOwner());
    jsonRowLCR.setPosition(rowLCR.getPosition());
    jsonRowLCR.setSourceDatabaseName(rowLCR.getSourceDatabaseName());
    jsonRowLCR.setTag(rowLCR.getTag());
    jsonRowLCR.setTransactionId(rowLCR.getTransactionId());
    jsonRowLCR.newValues = convertColumnValues(rowLCR.getNewValues());
    jsonRowLCR.oldValues = convertColumnValues(rowLCR.getOldValues());
    copyAttributes(rowLCR, jsonRowLCR);

    if (rowLCR.hasChunkData()) {
      jsonRowLCR.setChunkDataFlag(true);
      ChunkColumnValue chunkColumnValue;
      do {
        chunkColumnValue = xStreamOutput.receiveChunk();
        JsonChunkColumnValue jsonChunkColumnValue = JsonChunkColumnValue.buildChunk(chunkColumnValue);
        jsonRowLCR.chunkColumnValues.add(jsonChunkColumnValue);
      } while (!chunkColumnValue.isEndOfRow());
    }

    return jsonRowLCR;
  }

  public static void write(File file, JsonRowLCR change) throws IOException {
    try (OutputStream outputStream = new FileOutputStream(file)) {
      ObjectMapperFactory.instance.writeValue(outputStream, change);
    }
  }

  public static void write(OutputStream outputStream, JsonRowLCR change) throws IOException {
    ObjectMapperFactory.instance.writeValue(outputStream, change);
  }

  public static JsonRowLCR read(InputStream inputStream) throws IOException {
    return ObjectMapperFactory.instance.readValue(inputStream, JsonRowLCR.class);
  }

  public static JsonRowLCR read(File inputFile) throws IOException {
    try (FileInputStream inputStream = new FileInputStream(inputFile)) {
      return read(inputStream);
    }
  }

  public void chunkColumnValues(List<JsonChunkColumnValue> value) {
    this.chunkColumnValues = value;
  }

  public List<JsonChunkColumnValue> chunkColumnValues() {
    return this.chunkColumnValues;
  }

  @Override
  public ColumnValue[] getOldValues() {
    return this.oldValues;
  }

  @Override
  public void setOldValues(ColumnValue[] columnValues) {
    try {
      this.oldValues = convertColumnValues(columnValues);
    } catch (StreamsException e) {
      throw new DataException(e);
    }
  }

  @Override
  public ColumnValue[] getNewValues() {
    return this.newValues;
  }

  @Override
  public void setNewValues(ColumnValue[] columnValues) {
    try {
      this.newValues = convertColumnValues(columnValues);
    } catch (StreamsException e) {
      throw new DataException(e);
    }
  }

  @Override
  public boolean hasChunkData() {
    return this.hasChunkData;
  }

  @Override
  public void setChunkDataFlag(boolean b) {
    this.hasChunkData = b;
  }

  @Override
  public void setAttribute(Object o, Object o1) {
    Preconditions.checkState((o1 instanceof Datum), "value must be a datum");
    Datum datum = (Datum) o1;
    this.attributes.put(o, datum);
  }

  @Override
  public String getSourceDatabaseName() {
    return this.sourceDatabaseName;
  }

  @Override
  public void setSourceDatabaseName(String s) {
    this.sourceDatabaseName = s;
  }

  @Override
  public String getCommandType() {
    return this.commandType;
  }

  @Override
  public void setCommandType(String s) {
    this.commandType = s;
  }

  @Override
  public String getObjectOwner() {
    return this.objectOwner;
  }

  @Override
  public void setObjectOwner(String s) {
    this.objectOwner = s;
  }

  @Override
  public String getObjectName() {
    return this.objectName;
  }

  @Override
  public void setObjectName(String s) {
    this.objectName = s;
  }

  @Override
  public byte[] getTag() {
    return this.tag;
  }

  @Override
  public void setTag(byte[] bytes) {
    this.tag = bytes;
  }

  @Override
  public byte[] getPosition() {
    return this.position;
  }

  @Override
  public void setPosition(byte[] bytes) {
    this.position = bytes;
  }

  @Override
  public String getTransactionId() {
    return this.transactionID;
  }

  @Override
  public void setTransactionId(String s) {
    this.transactionID = s;
  }

  @Override
  public Object getAttribute(Object o) {
    return this.attributes.get(o);
  }

  @Override
  public DATE getSourceTime() {
    return new DATE(this.sourceTime);
  }

  @Override
  public void setSourceTime(DATE date) {
    sourceTime = date.toBytes();
  }

  @Override
  public void name(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return this.name;
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
  public static class JsonChunkColumnValue extends JsonColumnValue implements ChunkColumnValue {

    BigInteger chunkOffset;
    BigInteger chunkOperationSize;
    BigInteger charSetId;
    BigInteger chunkType;
    boolean lastChunk;
    boolean emptyChunk;
    boolean xmlDiff;
    boolean endOfRow;

    public static JsonChunkColumnValue buildChunk(ChunkColumnValue chunkColumnValue) throws StreamsException {
      JsonChunkColumnValue jsonChunkColumnValue = new JsonChunkColumnValue();
      copyValues(chunkColumnValue, jsonChunkColumnValue);
      jsonChunkColumnValue.setChunkOffset(chunkColumnValue.getChunkOffset());
      jsonChunkColumnValue.setChunkOperationSize(chunkColumnValue.getChunkOperationSize());
      jsonChunkColumnValue.setChunkType(chunkColumnValue.getChunkType());
      jsonChunkColumnValue.setEmptyChunk(chunkColumnValue.isEmptyChunk());
      jsonChunkColumnValue.setEndOfRow(chunkColumnValue.isEndOfRow());
      jsonChunkColumnValue.setLastChunk(chunkColumnValue.isLastChunk());
      jsonChunkColumnValue.setXMLDiff(chunkColumnValue.isXMLDiff());

      return jsonChunkColumnValue;
    }

    @Override
    public int getChunkType() {
      return this.chunkType.intValue();
    }

    @Override
    public void setChunkType(int i) throws StreamsException {
      this.chunkType = BigInteger.valueOf(i);
    }

    @Override
    public BigInteger getChunkOffset() {
      return this.chunkOffset;
    }

    @Override
    public void setChunkOffset(BigInteger v) {
      this.chunkOffset = v;
    }

    @Override
    public BigInteger getChunkOperationSize() {
      return this.chunkOperationSize;
    }

    @Override
    public void setChunkOperationSize(BigInteger v) {
      this.chunkOperationSize = v;
    }

    @Override
    public int getCharSetId() {
      return this.charSetId.intValue();
    }

    @Override
    public void setCharSetId(int i) {
      this.charSetId = BigInteger.valueOf(i);
    }

    @Override
    public boolean isLastChunk() {
      return this.lastChunk;
    }

    @Override
    public void setLastChunk(boolean b) {
      this.lastChunk = b;
    }

    @Override
    public boolean isEmptyChunk() {
      return this.emptyChunk;
    }

    @Override
    public void setEmptyChunk(boolean b) {
      this.emptyChunk = b;
    }

    @Override
    public boolean isXMLDiff() {
      return this.xmlDiff;
    }

    @Override
    public void setXMLDiff(boolean b) {
      this.xmlDiff = b;
    }

    @Override
    public boolean isEndOfRow() {
      return this.endOfRow;
    }

    @Override
    public void setEndOfRow(boolean b) {
      this.endOfRow = b;
    }
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
  public static class JsonColumnValue implements ColumnValue {

    String columnName;
    BigInteger columnDataType;
    boolean tdefFlag;
    boolean is32kData;
    BigInteger charsetId;
    Datum columnData;

    public static JsonColumnValue build(ColumnValue columnValue) throws StreamsException {
      JsonColumnValue jsonColumnValue = new JsonColumnValue();
      copyValues(columnValue, jsonColumnValue);
      jsonColumnValue.setColumnData(columnValue.getColumnData(), columnValue.getColumnDataType());
      return jsonColumnValue;
    }

    protected static void copyValues(ColumnValue columnValue, JsonColumnValue jsonColumnValue) {
      jsonColumnValue.set32kData(columnValue.is32kData());
      jsonColumnValue.setCharsetId(columnValue.getCharsetId());
      jsonColumnValue.setColumnName(columnValue.getColumnName());
      jsonColumnValue.setTDEFlag(columnValue.getTDEFlag());
    }

    @Override
    public String getColumnName() {
      return this.columnName;
    }

    @Override
    public void setColumnName(String s) {
      this.columnName = s;
    }

    @Override
    public Datum getColumnData() {
      return this.columnData;
    }

    @Override
    public void setColumnData(Datum datum, int i) throws StreamsException {
      this.columnDataType = BigInteger.valueOf(i);
      this.columnData = datum;
    }

    @Override
    public int getColumnDataType() {
      return this.columnDataType.intValue();
    }

    @Override
    public boolean getTDEFlag() {
      return this.tdefFlag;
    }

    @Override
    public void setTDEFlag(boolean b) {
      this.tdefFlag = b;
    }

    @Override
    public boolean is32kData() {
      return this.is32kData;
    }

    @Override
    public void set32kData(boolean b) {
      this.is32kData = b;
    }

    @Override
    public int getCharsetId() {
      return this.charsetId.intValue();
    }

    @Override
    public void setCharsetId(int i) {
      this.charsetId = BigInteger.valueOf(i);
    }
  }

}
