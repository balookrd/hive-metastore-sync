package com.wandisco.hivesync;

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
            "box2_hive", "box2_meta"})
    @BeforeSuite
    public void setupSuite(String hive1, String meta1, String hive2, String meta2) throws Exception {
        DockerComposeManager.start("src/test/resources/hive1.yaml");
        System.err.print("Connecting to hive1 ");
        for (int i = 0; i < 30; i++) {
            try {
                con1 = Tools.createNewHiveConnection(hive1);
                break;
            } catch (Exception e) {
                System.err.print(".");
                Thread.sleep(1_000);
            }
        }
        System.err.println();

        DockerComposeManager.start("src/test/resources/hive2.yaml");
        System.err.print("Connecting to hive2 ");
        for (int i = 0; i < 30; i++) {
            try {
                con2 = Tools.createNewHiveConnection(hive2);
                break;
            } catch (Exception e) {
                System.err.print(".");
                Thread.sleep(1_000);
            }
        }
        System.err.println();

        hms1 = Tools.createNewMetaConnection(meta1, false);
        hms2 = Tools.createNewMetaConnection(meta2, false);
        url1 = hive1;
        url2 = hive2;
        AbstractSuite.meta1 = meta1;
        AbstractSuite.meta2 = meta2;

        fullCleanup("BEFORE SUITE CLEANUP");
    }

    @AfterSuite
    public void cleanupSuite() throws Exception {
        fullCleanup("AFTER SUITE CLEANUP");
        con1.close();
        con2.close();
        hms1.close();
        hms2.close();
        DockerComposeManager.stop("src/test/resources/hive1.yaml");
        DockerComposeManager.stop("src/test/resources/hive2.yaml");
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
