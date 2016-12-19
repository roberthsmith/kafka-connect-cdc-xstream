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

import oracle.streams.LCR;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LCRKeyTest {

  @Test
  public void equals() {
    LCR lcr = mock(LCR.class);
    when(lcr.getSourceDatabaseName()).thenReturn("SourceDatabase");
    when(lcr.getObjectOwner()).thenReturn("ObjectOwner");
    when(lcr.getObjectName()).thenReturn("ObjectName");

    final LCRKey LCRKeyThis = new LCRKey(lcr);
    final LCRKey LCRKeyThat = new LCRKey(lcr);

    assertEquals(LCRKeyThis, LCRKeyThat);
    assertNotEquals(LCRKeyThis, new Object());

  }

  @Test
  public void compareTo() {
    LCR lcr = mock(LCR.class);
    when(lcr.getSourceDatabaseName()).thenReturn("SourceDatabase");
    when(lcr.getObjectOwner()).thenReturn("ObjectOwner");
    when(lcr.getObjectName()).thenReturn("ObjectName");

    final LCRKey LCRKeyThis = new LCRKey(lcr);
    final LCRKey LCRKeyThat = new LCRKey(lcr);

    assertEquals(0, LCRKeyThis.compareTo(LCRKeyThat));
  }

  @Test
  public void mapKey() {
    String EXPECTED_VALUE = "this is a test value.";
    Map<LCRKey, String> map = new HashMap<>();

    LCR lcr = mock(LCR.class);
    when(lcr.getSourceDatabaseName()).thenReturn("SourceDatabase");
    when(lcr.getObjectOwner()).thenReturn("ObjectOwner");
    when(lcr.getObjectName()).thenReturn("ObjectName");

    final LCRKey LCRKeyThis = new LCRKey(lcr);
    map.put(LCRKeyThis, EXPECTED_VALUE);
    assertEquals(EXPECTED_VALUE, map.get(LCRKeyThis));
  }

  @Test
  public void tostring() {
    LCR lcr = mock(LCR.class);
    when(lcr.getSourceDatabaseName()).thenReturn("SourceDatabase");
    when(lcr.getObjectOwner()).thenReturn("ObjectOwner");
    when(lcr.getObjectName()).thenReturn("ObjectName");

    final LCRKey LCRKeyThis = new LCRKey(lcr);
    System.out.println(LCRKeyThis);
  }
}
