package com.wandisco.hivesync.common;

import com.wandisco.hivesync.hive.HMSClient;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collection;

public class Tools {

    // The main function that checks if
    // two given strings match. The first string
    // may contain wildcard characters
    public static boolean match(String pattern, String string) {

        // If we reach at the end of both strings,
        // we are done
        if (pattern.isEmpty() && string.isEmpty())
            return true;

        // Make sure to eliminate consecutive '*'
        if (pattern.length() > 1 && pattern.charAt(0) == '*') {
            int i = 0;
            while (i + 1 < pattern.length() && pattern.charAt(i + 1) == '*')
                i++;
            pattern = pattern.substring(i);
        }

        // Make sure that the characters after '*'
        // are present in second string.
        // This function assumes that the first
        // string will not contain two consecutive '*'
        if (pattern.length() > 1 && pattern.charAt(0) == '*' &&
                string.isEmpty())
            return false;

        // If the first string contains '?',
        // or current characters of both strings match
        if ((pattern.length() > 1 && pattern.charAt(0) == '?') ||
                (!pattern.isEmpty() && !string.isEmpty() &&
                        pattern.charAt(0) == string.charAt(0)))
            return match(pattern.substring(1),
                    string.substring(1));

        // If there is *, then there are two possibilities
        // a) We consider current character of second string
        // b) We ignore current character of second string.
        if (!pattern.isEmpty() && pattern.charAt(0) == '*')
            return match(pattern.substring(1), string) ||
                    match(pattern, string.substring(1));
        return false;
    }

    public static boolean match(Collection<String> pattern, String string) {
        for (String patternItem : pattern) {
            if (match(patternItem, string)) {
                return true;
            }
        }
        return false;
    }

    public static Connection createNewHiveConnection(String connectionString) throws Exception {
        if (connectionString.startsWith("jdbc:hive2"))
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        else
            Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
        return DriverManager.getConnection(connectionString);
    }

    public static HMSClient createNewMetaConnection(String connectionString, boolean metaSasl) throws MetaException {
        HiveConf conf = new HiveConf();
        conf.set("hive.metastore.uris", connectionString);
        conf.set("hive.metastore.sasl.enabled", metaSasl ? "true" : "false");
        return new HMSClient(conf);
    }
}
