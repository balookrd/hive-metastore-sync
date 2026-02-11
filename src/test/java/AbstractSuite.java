package test.java;

import com.wandisco.hivesync.common.Tools;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Parameters;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class AbstractSuite {
    private static Connection con1;
    private static Connection con2;
    private static String url1;
    private static String url2;

    @Parameters({"box1_host", "box1_jdbc_port", "box2_host", "box2_jdbc_port",
            "hadoop_home", "hive_home"})
    @BeforeSuite
    public void setupSuite(String host1, String port1, String host2,
                           String port2, String hadoopHome, String hiveHome)
            throws Exception {
        url1 = "jdbc:hive2://" + host1 + ":" + port1;
        url2 = "jdbc:hive2://" + host2 + ":" + port2;
        con1 = Tools.createNewConnection(url1);
        con2 = Tools.createNewConnection(url2);
        fullCleanup("BEFORE SUITE CLEANUP");
    }

    public static void fullCleanup(String name) throws SQLException {
        fullCleanup(con1, name, url1);
        fullCleanup(con2, name, url2);
    }

    private static void fullCleanup(Connection con, String name, String url) throws SQLException {
        System.err.println(name + ": " + url);
        Statement stm1 = con.createStatement();
        Statement stm2 = con.createStatement();
        Statement stm3 = con.createStatement();
        ResultSet rs1 = stm1.executeQuery("show databases");
        while (rs1.next()) {
            String db = rs1.getString(1);
            ResultSet rs2 = stm2.executeQuery("show tables in " + db);
            while (rs2.next()) {
                String tbl = rs2.getString(1);
                System.err.println("DROP TABLE: " + db + "." + tbl);
                stm3.execute("drop table " + db + "." + tbl);
            }
            if (!"default".equals(db)) {
                System.err.println("DROP DATABASE: " + db);
                stm3.execute("drop database " + db);
            }
        }
    }

    public static Connection getCon1() {
        return con1;
    }

    public static Connection getCon2() {
        return con2;
    }

    @AfterSuite
    public void cleanupSuite() throws SQLException {
        fullCleanup("AFTER SUITE CLEANUP");
        try {
            con1.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            con2.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static String getUrl1() {
        return url1;
    }

    public static String getUrl2() {
        return url2;
    }
}
