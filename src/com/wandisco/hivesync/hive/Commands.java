package com.wandisco.hivesync.hive;

import com.wandisco.hivesync.common.Tools;
import com.wandisco.hivesync.main.HiveSync;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class Commands {

    private static final Logger LOG = LogManager.getLogger(HiveSync.class);

    private static String dryRunFile = null;

    public static void setDryRunFile(String name) {
        dryRunFile = name;
    }

    public static void createTable(Connection hive, HMSClient hms, TableInfo table) throws Exception {
        Commands.executeQuery(hive, table.getCreateCommand());
        Commands.createPartitions(hms, table, table.getPartitions());
    }

    public static void dropTable(Connection hive, TableInfo table) throws Exception {
        Commands.executeQuery(hive, "DROP TABLE " + table.getName());
    }

    public static ArrayList<TableInfo> getTables(Connection hive, HMSClient hms, String dbName) throws Exception {
        ArrayList<TableInfo> tablesInfo = new ArrayList<>();
        List<String> srcTables = Commands.forceExecuteQuery(hive, "SHOW TABLES IN " + dbName);
        for (String srcTable : srcTables) {
            List<String> createCommand = queryCreateCommand(hive, dbName + "." + srcTable);
            boolean isManaged = queryIsManaged(hive, dbName + "." + srcTable);
            List<PartitionInfo> partitions = queryPartitions(hms, dbName, srcTable);
            TableInfo ti = new TableInfo(
                    hms.getTable(dbName, srcTable),
                    dbName + "." + srcTable,
                    createCommand,
                    partitions,
                    isManaged);
            tablesInfo.add(ti);
        }
        return tablesInfo;
    }

    public static void updatePartitions(HMSClient hms, TableInfo src, TableInfo dst) throws Exception {
        List<PartitionInfo> newParts = new ArrayList<>();
        for (PartitionInfo srcPart : src.getPartitions()) {
            if (findPartition(dst.getPartitions(), srcPart.getName()) == null) {
                newParts.add(srcPart);
            }
        }
        createPartitions(hms, dst, newParts);
        List<PartitionInfo> delParts = new ArrayList<>();
        for (PartitionInfo dstPart : dst.getPartitions()) {
            if (findPartition(src.getPartitions(), dstPart.getName()) == null) {
                delParts.add(dstPart);
            }
        }
        dropPartitions(hms, dst, delParts);
    }

    private static PartitionInfo findPartition(List<PartitionInfo> parts, String partititon) {
        for (PartitionInfo pi : parts) {
            if (pi.getName().equalsIgnoreCase(partititon)) {
                return pi;
            }
        }
        return null;
    }

    private static List<String> queryCreateCommand(Connection hive, String srcTable) throws Exception {
        return Commands.forceExecuteQuery(hive, "SHOW CREATE TABLE " + srcTable);
    }

    private static boolean queryIsManaged(Connection con, String tableName) throws Exception {
        List<String> descFormatted = Commands.forceExecuteQuery(con, "DESC FORMATTED " + tableName);
        for (String s : descFormatted) {
            if (s.startsWith("Table Type:")) {
                if (s.contains("MANAGED_TABLE")) {
                    return true;
                }
            }
        }
        return false;
    }

    private static List<PartitionInfo> queryPartitions(Connection hive, String tableName) throws Exception {
        try {
            return Commands.forceExecuteQuery(hive, "SHOW PARTITIONS " + tableName)
                    .stream()
                    .map(part -> new PartitionInfo(part, Collections.emptyList(), ""))
                    .collect(Collectors.toList());
        } catch (SQLException e) {
            return Collections.emptyList();
        }
    }

    private static List<PartitionInfo> queryPartitions(HMSClient hms, String dbName, String tableName) throws Exception {
        Table table = hms.getTable(dbName, tableName);
        List<String> partColumns = table
                .getPartitionKeys()
                .stream()
                .map(FieldSchema::getName)
                .collect(Collectors.toList());
        List<PartitionInfo> al = new ArrayList<>();
        List<String> partNames = hms.listPartitionNames(dbName, tableName, (short) -1);
        int batchSize = 1000;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        for (int i = 0; i < partNames.size(); i += batchSize) {
            List<String> batch = partNames.subList(i, Math.min(i + batchSize, partNames.size()));
            pool.submit(() -> {
                try (HMSClient hmsClient = hms.createClient()) {
                    List<PartitionInfo> pList = hmsClient.getPartitionsByNames(dbName, tableName, batch)
                            .stream()
                            .map(partition ->
                                    new PartitionInfo(FileUtils.makePartName(partColumns, partition.getValues()),
                                            partition.getValues(), partition.getSd().getLocation())
                            ).collect(Collectors.toList());
                    synchronized (al) {
                        al.addAll(pList);
                    }
                }
                return null;
            });
        }
        pool.shutdown();
        pool.awaitTermination(60, TimeUnit.MINUTES);
        return al;
    }

    private static void createPartitions(Connection hive, TableInfo table, List<PartitionInfo> parts) throws Exception {
        int batchSize = 100;
        for (int i = 0; i < parts.size(); i += batchSize) {
            List<PartitionInfo> batch = parts.subList(i, Math.min(i + batchSize, parts.size()));
            StringBuilder query = new StringBuilder();
            query.append("ALTER TABLE ").append(table.getName()).append(" ADD IF NOT EXISTS ");
            boolean notFirst = false;
            for (PartitionInfo partition : batch) {
                LOG.debug("- create partition: " + partition);
                if (notFirst) {
                    query.append(",");
                } else {
                    notFirst = true;
                }
                query.append("PARTITION (").append(partition.getNameTranslated()).append(")");
                if (!partition.getLocation().isEmpty()) {
                    query.append(" LOCATION '").append(partition.getLocation()).append("'");
                }
            }
            Commands.executeQuery(hive, query.toString());
        }
    }

    private static void createPartitions(HMSClient hms, TableInfo tableInfo, List<PartitionInfo> parts) throws Exception {
        int batchSize = 1000;
        ExecutorService pool = Executors.newFixedThreadPool(6);
        for (int i = 0; i < parts.size(); i += batchSize) {
            List<PartitionInfo> batch = parts.subList(i, Math.min(i + batchSize, parts.size()));
            pool.submit(() -> {
                List<Partition> list = batch.stream()
                        .map(p -> createPartition(tableInfo.getTable(), p.getValues(), p.getLocation()))
                        .collect(Collectors.toList());
                try (HMSClient hmsClient = hms.createClient()) {
                    hmsClient.add_partitions(list, true, false);
                }
                return null;
            });
        }
        pool.shutdown();
        pool.awaitTermination(60, TimeUnit.MINUTES);
    }

    private static void dropPartitions(Connection hive, TableInfo table, List<PartitionInfo> parts) throws Exception {
        int batchSize = 100;
        for (int i = 0; i < parts.size(); i += batchSize) {
            List<PartitionInfo> batch = parts.subList(i, Math.min(i + batchSize, parts.size()));
            StringBuilder query = new StringBuilder();
            query.append("ALTER TABLE ").append(table.getName()).append(" DROP IF EXISTS ");
            boolean notFirst = false;
            for (PartitionInfo partition : batch) {
                LOG.debug("- drop partition: " + partition);
                if (notFirst) {
                    query.append(",");
                } else {
                    notFirst = true;
                }
                query.append("PARTITION (").append(partition.getNameTranslated()).append(")");
            }
            Commands.executeQuery(hive, query.toString());
        }
    }

    private static void dropPartitions(HMSClient hms, TableInfo table, List<PartitionInfo> parts) throws Exception {
        int batchSize = 1000;
        ExecutorService pool = Executors.newFixedThreadPool(6);
        PartitionDropOptions options = new PartitionDropOptions()
                .deleteData(false)
                .ifExists(true)
                .purgeData(false)
                .returnResults(false);
        for (int i = 0; i < parts.size(); i += batchSize) {
            List<PartitionInfo> batch = parts.subList(i, Math.min(i + batchSize, parts.size()));
            pool.submit(() -> {
                try (HMSClient hmsClient = hms.createClient()) {
                    for (PartitionInfo p : batch) {
                        hmsClient.dropPartition(
                                table.getTable().getDbName(),
                                table.getTable().getTableName(),
                                p.getValues(),
                                options
                        );
                    }
                }
                return null;
            });
        }
        pool.shutdown();
        pool.awaitTermination(60, TimeUnit.MINUTES);
    }

    public static String getFsDefaultName(Connection con) throws Exception {
        String result = Tools.join(Commands.forceExecuteQuery(con, "SET fs.default.name"));
        if (!result.startsWith("fs.default.name=")) {
            throw new Exception("Can't detect file system");
        }
        return result.replaceAll("fs.default.name=", "");
    }

    public static List<String> executeQuery(Connection con, String query) throws Exception {
        if (dryRunFile != null) {
            LOG.trace("Dry run: " + query);
            try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(dryRunFile, true)))) {
                out.println(query);
            }
            return Collections.emptyList();
        } else {
            return forceExecuteQuery(con, query);
        }
    }

    private static List<String> forceExecuteQuery(Connection con, String query)
            throws Exception {
        List<String> al = new ArrayList<>();
        Statement s = con.createStatement();
        ResultSet rs = null;
        try {
            LOG.trace("Executing: " + query);
            s.execute(query);
            rs = s.getResultSet();

            if (rs == null) {
                LOG.trace("Empty result.");
            } else {
                LOG.trace("Result:");
                int colN = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    StringBuilder sb = new StringBuilder();
                    for (int cid = 1; cid <= colN; cid++) {
                        sb.append(rs.getString(cid));
                        if (cid < colN) {
                            sb.append("\t");
                        }
                    }
                    al.add(sb.toString());
                    LOG.trace(sb.toString());
                }
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
            s.close();
        }
        return al;
    }

    public static List<String> getDatabases(Connection connection, String pattern) throws Exception {
        LOG.trace("Getting database list");
        return Commands.forceExecuteQuery(connection, "SHOW DATABASES LIKE '" + pattern + "'");
    }

    public static void createDatabase(Connection con, String db) throws Exception {
        LOG.trace("Creating database");
        executeQuery(con, "CREATE DATABASE " + db);
    }

    public static Partition createPartition(
            Table table,
            List<String> values,
            String customLocation) {

        StorageDescriptor sdCopy = table.getSd().deepCopy();
        sdCopy.setLocation(customLocation);

        Partition partition = new Partition();
        partition.setDbName(table.getDbName());
        partition.setTableName(table.getTableName());
        partition.setValues(values);
        partition.setSd(sdCopy);
        partition.setCreateTime((int) (System.currentTimeMillis() / 1000));
        partition.setLastAccessTime(0);

        // Опционально (чтобы не раздувать метаданные)
        partition.setParameters(Collections.emptyMap());

        return partition;
    }
}
