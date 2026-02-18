package com.wandisco.hivesync;

import com.wandisco.hivesync.hive.HMSClient;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbstractTest {

    protected Connection con1;
    protected Connection con2;
    protected HMSClient hms1;
    protected HMSClient hms2;

    protected String meta1;
    protected String meta2;

    @BeforeClass
    public void init() {
        con1 = AbstractSuite.getCon1();
        con2 = AbstractSuite.getCon2();
        hms1 = AbstractSuite.getHms1();
        hms2 = AbstractSuite.getHms2();
        meta1 = AbstractSuite.getMeta1();
        meta2 = AbstractSuite.getMeta2();
    }

    protected void checkResult(Statement stmt, String query, String[] strings) throws SQLException {
        ResultSet rs = stmt.executeQuery(query);
        for (String string : strings) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), string);
        }
        Assert.assertFalse(rs.next(), "unexpected value: '" + rs.getString(1) + "'");
    }
}
