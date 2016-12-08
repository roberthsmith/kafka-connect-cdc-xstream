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

import freemarker.template.TemplateException;
import io.confluent.kafka.connect.cdc.xstream.schema.TableMetadataProvider;
import io.confluent.kafka.connect.cdc.xstream.schema.TableMetadataProviderTest;
import oracle.streams.RowLCR;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaGeneratorTest {

  @Test
  public void generate() throws SQLException, IOException, TemplateException {
    XStreamSourceConnectorConfig config = new XStreamSourceConnectorConfig(XStreamSourceConnectorConfigTest.settings());

    RowLCR row = mock(RowLCR.class);
    when(row.getSourceDatabaseName()).thenReturn("SourceDatabase");
    when(row.getObjectOwner()).thenReturn("ObjectOwner");
    when(row.getObjectName()).thenReturn("ObjectName");

    Connection connection = ConnectionMock.mockConnection();
    TableMetadataProvider tableMetadataProvider = TableMetadataProviderTest.mockTableMetadataProvider();


//    SchemaGenerator generator = new SchemaGenerator(config, connection, tableMetadataProvider);
//
//    SchemaPair schemaPair = generator.generate(row);
//    assertNotNull(schemaPair);
//    assertNotNull(schemaPair.getKey());
//    assertNotNull(schemaPair.getValue());


    //TODO: Figure out how to verify close.
//    verify(columnsResultSet, times(1)).close();
//    verify(keysResultSet, times(1)).close();
  }
}
