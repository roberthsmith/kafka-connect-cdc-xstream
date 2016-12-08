package io.confluent.kafka.connect.cdc.xstream;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;

import java.util.Map;

class Position {
  public static Map<String, ?> partition = ImmutableMap.of(
      "partiion", 1
  );

  public static byte[] toBytes(String s) {
    return BaseEncoding.base32Hex().decode(s);
  }

  public static String toString(byte[] b) {
    return BaseEncoding.base32Hex().encode(b);
  }
}
