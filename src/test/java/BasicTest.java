package test.java;

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
        Statement s1 = con1.createStatement();
        s1.execute("create table table1 (col1 int)");
        s1.execute("create table table2 (col1 int) partitioned by(col2 date)");
        s1.execute("alter table table2 add partition(col2='1980-12-24')");
        s1.execute("alter table table2 add partition(col2='1976-11-24')");
        s1.close();

        Statement s2 = con2.createStatement();
        s2.execute("create table table2 (col1 int) partitioned by(col2 date)");
        s2.execute("alter table table2 add partition(col2='2026-01-01')");
        s2.execute("create table table3 (col1 int)");
        s2.close();
    }

    @Test
    public void f() throws Exception {
        List<String> dbs = Collections.singletonList("default");
        HiveSync hs = new HiveSync(meta1, meta2, false, dbs, Collections.singletonList("*"));
        hs.execute();

        Statement s2 = con2.createStatement();
        checkResult(s2, "show tables", new String[]{"table1", "table2"});
        checkResult(s2, "describe table1", new String[]{"col1"});
        checkResult(s2, "describe table2", new String[]{"col1", "col2", "",
                "# Partition Information", "# col_name", "col2"});
        checkResult(s2, "show partitions table2", new String[]{"col2=1976-11-24", "col2=1980-12-24"});
        s2.close();
    }

    @AfterClass
    public void cleanup() throws SQLException {
        AbstractSuite.fullCleanup("AFTER TEST CLEANUP");
    }
}
