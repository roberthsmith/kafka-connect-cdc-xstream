package io.confluent.kafka.connect.cdc.xstream;

import com.google.common.collect.ImmutableSet;
import org.hamcrest.core.IsEqual;
import org.junit.Before;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import static org.junit.Assert.assertThat;

@Disabled
public class Oracle11gKeyMetadataProviderTests extends Oracle11gTest {
  private static final Logger log = LoggerFactory.getLogger(Oracle11gKeyMetadataProviderTests.class);

  Connection connection;
  Oracle11gKeyMetadataProvider keyMetadataProvider;

  @Before
  public void before() throws SQLException {
    this.connection = Utils.openConnection(jdbcUrl, DockerUtils.USERNAME, DockerUtils.PASSWORD);
    this.keyMetadataProvider = new Oracle11gKeyMetadataProvider(this.connection);
  }

  @Test
  public void findPrimaryKey() throws SQLException {
    Set<String> expectedKeys = ImmutableSet.of("USER_ID");
    Set<String> actualKeys = this.keyMetadataProvider.findPrimaryKey("CDC_TESTING", "PRIMARY_KEY_TABLE");
    assertThat("actualKeys did not match.", actualKeys, IsEqual.equalTo(expectedKeys));

    expectedKeys = ImmutableSet.of();
    actualKeys = this.keyMetadataProvider.findPrimaryKey("CDC_TESTING", "UNIQUE_INDEX_TABLE");
    assertThat("actualKeys did not match.", actualKeys, IsEqual.equalTo(expectedKeys));
  }

  @Test
  public void findUniqueKey() throws SQLException {
    Set<String> expectedKeys = ImmutableSet.of("FIRST_COLUMN", "SECOND_COLUMN");
    Set<String> actualKeys = this.keyMetadataProvider.findUniqueKey("cdc_testing", "UNIQUE_INDEX_TABLE");
    assertThat("actualKeys did not match.", actualKeys, IsEqual.equalTo(expectedKeys));

    expectedKeys = ImmutableSet.of();
    actualKeys = this.keyMetadataProvider.findUniqueKey("CDC_TESTING", "NO_INDEXES");
    assertThat("actualKeys did not match.", actualKeys, IsEqual.equalTo(expectedKeys));
  }

  @Test
  public void findKeys() throws SQLException {
    Set<String> expectedKeys = ImmutableSet.of("FIRST_COLUMN", "SECOND_COLUMN");
    Set<String> actualKeys = this.keyMetadataProvider.findKeys("cdc_testing", "UNIQUE_INDEX_TABLE");
    assertThat("actualKeys did not match.", actualKeys, IsEqual.equalTo(expectedKeys));
  }
}
