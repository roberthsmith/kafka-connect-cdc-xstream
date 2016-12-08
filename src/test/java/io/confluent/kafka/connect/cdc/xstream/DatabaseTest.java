package io.confluent.kafka.connect.cdc.xstream;

import org.flywaydb.core.Flyway;
import org.junit.Test;

public class DatabaseTest {

  @Test
  public void foo() {
    Flyway flyway = new Flyway();
    flyway.setBaselineOnMigrate(true);

    // Point it to the database
    flyway.setDataSource("jdbc:oracle:thin:@//confluent:1521/ORCLPDB1", "sys as sysdba", "password");


    // Start the db.migration.migration
//    flyway.migrate();
  }
}
