package io.confluent.kafka.connect.cdc.xstream;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.connect.errors.DataException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

abstract class KeyMetadataProvider {
  protected final Connection connection;
  final Cache<SchemaKey, Set<String>> keyCache;

  KeyMetadataProvider(Connection connection) {
    this.connection = connection;

    this.keyCache = CacheBuilder.newBuilder()
        .expireAfterWrite(5, TimeUnit.MINUTES)
        .build();
  }

  abstract Set<String> findPrimaryKey(String schemaName, String tableName) throws SQLException;

  abstract Set<String> findUniqueKey(String schemaName, String tableName) throws SQLException;

  Set<String> findKeys(final String schemaName, final String tableName) {
    SchemaKey schemaKey = new SchemaKey(schemaName, tableName);
    try {
      Set<String> keys = this.keyCache.get(schemaKey, new Callable<Set<String>>() {
        @Override
        public Set<String> call() throws Exception {
          Set<String> result = findPrimaryKey(schemaName, tableName);
          if (null != result && !result.isEmpty()) {
            return result;
          }
          result = findUniqueKey(schemaName, tableName);
          if (null != result && !result.isEmpty()) {
            return result;
          }
          return ImmutableSet.of();
        }
      });
      return keys;
    } catch (ExecutionException e) {
      throw new DataException(
          String.format("Could not determine primary keys for {}", schemaKey),
          e
      );
    }
  }

}
