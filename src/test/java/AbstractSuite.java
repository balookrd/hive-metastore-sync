package test.java;

import com.wandisco.hivesync.common.Tools;
import com.wandisco.hivesync.hive.HMSClient;
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
    private static HMSClient hms1;
    private static HMSClient hms2;

    private static String url1;
    private static String url2;
    private static String meta1;
    private static String meta2;

    @Parameters({"box1_hive", "box1_meta",
            "box2_hive", "box2_meta",
            "hadoop_home", "hive_home"})
    @BeforeSuite
    public void setupSuite(String hive1, String meta1,
                           String hive2, String meta2,
                           String hadoopHome, String hiveHome)
            throws Exception {
        con1 = Tools.createNewHiveConnection(hive1);
        con2 = Tools.createNewHiveConnection(hive2);
        hms1 = Tools.createNewMetaConnection(meta1, false);
        hms2 = Tools.createNewMetaConnection(meta2, false);
        url1 = hive1;
        url2 = hive2;
        AbstractSuite.meta1 = meta1;
        AbstractSuite.meta2 = meta2;
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

    public static HMSClient getHms1() {
        return hms1;
    }

    public static HMSClient getHms2() {
        return hms2;
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

    public static String getMeta1() {
        return meta1;
    }

    public static String getMeta2() {
        return meta2;
    }
}
