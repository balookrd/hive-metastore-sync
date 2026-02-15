package com.wandisco.hivesync;

import com.wandisco.hivesync.main.HiveSync;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

public class BasicTest extends AbstractTest {
    @BeforeClass
    public void setup() throws Exception {
        AbstractSuite.fullCleanup("BEFORE TEST CLEANUP");
        try (Statement s1 = con1.createStatement(); Statement s2 = con2.createStatement()) {
            s1.execute("create table table1 (col1 int)");
            s1.execute("create table table2 (col1 int) partitioned by(col2 date)");
            s1.execute("alter table table2 add partition(col2='1980-12-24')");
            s1.execute("alter table table2 add partition(col2='1976-11-24')");
            s1.execute("insert into table2 partition(col2='1976-11-24') values (1),(2),(3)");

            s2.execute("create table table2 (col1 int) partitioned by(col2 date)");
            s2.execute("alter table table2 add partition(col2='2026-01-01')");
            s2.execute("insert into table2 partition(col2='2026-01-01') values(11),(12),(13)");
            s2.execute("create table table3 (col1 int)");
        }
    }

    private void checkTableContent(Statement s) throws SQLException {
        checkResult(s, "show tables", new String[]{"table1", "table2", "table3"});
        checkResult(s, "describe table1", new String[]{"col1"});
        checkResult(s, "describe table2", new String[]{"col1", "col2", "",
                "# Partition Information", "# col_name", "col2"});
        checkResult(s, "show partitions table2", new String[]{"col2=1976-11-24",
                "col2=1980-12-24", "col2=2026-01-01"});
    }

    @Test
    public void check1ReplicationSync() throws Exception {
        List<String> dbs = Collections.singletonList("default");
        HiveSync hs = new HiveSync(meta1, meta2, false, dbs, Collections.singletonList("*"));
        try (Statement s1 = con1.createStatement(); Statement s2 = con2.createStatement()) {
            hs.execute();
            checkTableContent(s1);
            checkTableContent(s2);
        }
    }

    @Test
    public void check2ReplicationDropTable() throws Exception {
        List<String> dbs = Collections.singletonList("default");
        HiveSync hs = new HiveSync(meta1, meta2, false, dbs, Collections.singletonList("*"));
        try (Statement s1 = con1.createStatement(); Statement s2 = con2.createStatement()) {
            s2.execute("drop table table3");
            hs.execute();
            checkResult(s1, "show tables", new String[]{"table1", "table2"});
            checkResult(s2, "show tables", new String[]{"table1", "table2"});
        }
    }

    @Test
    public void check3ReplicationDropPartition() throws Exception {
        List<String> dbs = Collections.singletonList("default");
        HiveSync hs = new HiveSync(meta1, meta2, false, dbs, Collections.singletonList("*"));
        try (Statement s1 = con1.createStatement(); Statement s2 = con2.createStatement()) {
            s1.execute("alter table table2 drop partition(col2='1976-11-24')");

            s2.execute("alter table table2 drop partition(col2='1980-12-24')");
            s2.execute("alter table table2 drop partition(col2='2026-01-01')");

            hs.execute();

            checkResult(s1, "show partitions table2", new String[]{"col2=1980-12-24"});
            checkResult(s2, "show partitions table2", new String[]{"col2=1980-12-24"});
        }
    }

    @AfterClass
    public void cleanup() throws SQLException {
        AbstractSuite.fullCleanup("AFTER TEST CLEANUP");
    }
}
