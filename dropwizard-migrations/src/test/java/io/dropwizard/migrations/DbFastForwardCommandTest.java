package io.dropwizard.migrations;

import com.google.common.collect.ImmutableMap;
import net.sourceforge.argparse4j.inf.Namespace;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import static org.assertj.core.api.Assertions.assertThat;

public class DbFastForwardCommandTest extends AbstractMigrationTest {

    private DbFastForwardCommand<TestMigrationConfiguration> fastForwardCommand = new DbFastForwardCommand<>(
        TestMigrationConfiguration::getDataSource, TestMigrationConfiguration.class, "migrations.xml");
    private TestMigrationConfiguration conf;
    private String databaseUrl;
    private DBI dbi;

    @Before
    public void setUp() throws Exception {
        databaseUrl = "jdbc:h2:" + createTempFile();
        conf = createConfiguration(databaseUrl);
        dbi = new DBI(databaseUrl, "sa", "");
    }

    @Test
    public void testFastForwardFirst() throws Exception {
        // Create the "persons" table manually
        try (Handle handle = dbi.open()) {
            handle.execute("create table persons(id int, name varchar(255))");
        }

        // Fast-forward one change
        fastForwardCommand.run(null, new Namespace(ImmutableMap.of("all", false, "dry-run", false)), conf);

        // 2nd and 3rd migrations will performed
        new DbMigrateCommand<>(
            TestMigrationConfiguration::getDataSource, TestMigrationConfiguration.class, "migrations.xml")
            .run(null, new Namespace(ImmutableMap.of()), conf);
    }

    @Test
    public void testFastForwardAll() throws Exception {
        // Create the "persons" table manually and add some data
        try (Handle handle = dbi.open()) {
            handle.execute("create table persons(id int, name varchar(255))");
            handle.execute("insert into persons (id, name) values (12, 'Greg Young')");
        }

        // Fast-forward all the changes
        fastForwardCommand.run(null, new Namespace(ImmutableMap.of("all", true, "dry-run", false)), conf);

        // No migrations is performed
        new DbMigrateCommand<>(
            TestMigrationConfiguration::getDataSource, TestMigrationConfiguration.class, "migrations.xml")
            .run(null, new Namespace(ImmutableMap.of()), conf);

        // Nothing is added to the persons table
        try (Handle handle = dbi.open()) {
            assertThat(handle.createQuery("select count(*) from persons")
                .mapTo(Integer.class)
                .first()).isEqualTo(1);
        }
    }
}
