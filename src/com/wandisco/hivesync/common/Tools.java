package com.wandisco.hivesync.common;

import com.wandisco.hivesync.hive.HMSClient;
import org.apache.hadoop.hive.conf.HiveConf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Utility class
 *
 * @author Oleg Danilov
 *
 */
public class Tools {

    /**
     * Read file line by line and return list of strings
     **/
    public static List<String> readTextFile(String filename) {
        ArrayList<String> list = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            String line;
            while ((line = br.readLine()) != null) {
                list.add(line);
            }
            br.close();
        } catch (Exception e) {
            System.err.println("Can't read file " + filename);
        }
        return list;
    }

    public static String join(List<String> list) {
        StringBuilder sb = new StringBuilder();
        Iterator<String> iter = list.iterator();
        while (iter.hasNext()) {
            sb.append(iter.next());
            if (iter.hasNext()) {
                sb.append(System.lineSeparator());
            }
        }
        return sb.toString();
    }

    public static Connection createNewHiveConnection(String connectionString) throws Exception {
        if (connectionString.startsWith("jdbc:hive2"))
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        else
            Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
        return DriverManager.getConnection(connectionString);
    }

    public static HMSClient createNewMetaConnection(String connectionString) throws Exception {
        HiveConf conf = new HiveConf();
        conf.set("hive.metastore.uris", connectionString);
        return new HMSClient(conf);
    }
}
