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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.apache.kafka.connect.data.Schema;

class SchemaKey implements Comparable<SchemaKey> {
  final Schema.Type type;
  final String logicalName;

  public SchemaKey(Schema schema) {
    this.type = schema.type();
    this.logicalName = schema.name();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("type", this.type)
        .add("logicalName", this.logicalName)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.type, this.logicalName);
  }

  @Override
  public int compareTo(SchemaKey that) {
    if (null == that) {
      return 1;
    }
    return ComparisonChain.start()
        .compare(this.type, that.type)
        .compare(this.logicalName, that.logicalName, Ordering.natural().nullsFirst())
        .result();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SchemaKey) {
      return compareTo((SchemaKey) obj) == 0;
    } else {
      return false;
    }
  }
}
