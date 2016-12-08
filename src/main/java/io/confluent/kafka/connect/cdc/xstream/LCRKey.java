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
package io.confluent.kafka.connect.cdc.xstream;


import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import oracle.streams.LCR;

class LCRKey implements Comparable<LCRKey> {
  final String sourceDatabaseName;
  final String objectOwner;
  final String objectName;

  LCRKey(LCR lcr) {
    this.sourceDatabaseName = lcr.getSourceDatabaseName();
    this.objectOwner = lcr.getObjectOwner();
    this.objectName = lcr.getObjectName();
  }

  @Override
  public int compareTo(LCRKey that) {
    return ComparisonChain.start()
        .compare(this.sourceDatabaseName, that.sourceDatabaseName)
        .compare(this.objectOwner, that.objectOwner)
        .compare(this.objectName, that.objectName)
        .result();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(LCRKey.class)
        .add("sourceDatabaseName", this.sourceDatabaseName)
        .add("objectOwner", this.objectOwner)
        .add("objectName", this.objectName)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        this.sourceDatabaseName,
        this.objectOwner,
        this.objectName
    );
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof LCRKey) {
      LCRKey that = (LCRKey) obj;
      return 0 == this.compareTo(that);
    } else {
      return false;
    }
  }
}
