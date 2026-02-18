package com.wandisco.hivesync.hive;

import com.wandisco.hivesync.common.Tools;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.wandisco.hivesync.common.Tools.awaitTermination;

public abstract class Commands {

    private static final Logger LOG = LogManager.getLogger(Commands.class);

    private static boolean dryRun = false;

    public static void setDryRun(boolean isDryRun) {
        dryRun = isDryRun;
    }

    public static List<String> getDatabases(HMSClient hms, String pattern) throws TException {
        LOG.trace("Getting database list");
        return hms.getAllDatabases().stream()
                .filter(d -> Tools.match(pattern, d))
                .collect(Collectors.toList());
    }

    public static void createDatabase(HMSClient hms, Database db) throws TException {
        LOG.trace("Creating database: {}", db.getName());
        Database dbCopy = db.deepCopy();
        dbCopy.unsetParameters();
        if (dryRun) {
            LOG.info("Creating database: {}", db.getName());
        } else {
            hms.createDatabase(dbCopy);
        }
    }

    public static ArrayList<TableInfo> getTables(HMSClient hms, String dbName) throws TException {
        LOG.trace("Getting table list: {}", dbName);
        ArrayList<TableInfo> tablesInfo = new ArrayList<>();
        List<String> tables = hms.getAllTables(dbName);
        for (String srcTable : tables) {
            List<PartitionInfo> partitions = queryPartitions(hms, dbName, srcTable);
            TableInfo ti = new TableInfo(hms.getTable(dbName, srcTable), partitions);
            tablesInfo.add(ti);
        }
        return tablesInfo;
    }

    public static void createTable(HMSClient hms, TableInfo table) throws TException {
        LOG.trace("Creating table: {}.{}", table.getDb(), table.getName());
        Table tableCopy = table.getTable().deepCopy();
        tableCopy.setTableType("EXTERNAL_TABLE");
        Map<String, String> params = new HashMap<>();
        params.put("EXTERNAL", "TRUE");
        // params.put("external.table.purge", "FALSE");
        params.put("replicated", "true");
        tableCopy.setParameters(params);
        if (dryRun) {
            LOG.info("Creating table: {}.{}", table.getDb(), table.getName());
        } else {
            hms.createTable(tableCopy);
        }
        if (table.isPartitioned()) {
            createPartitions(hms, table, table.getPartitions());
        }
    }

    public static void dropTable(HMSClient hms, TableInfo table) throws TException {
        LOG.trace("Dropping table: {}.{}", table.getDb(), table.getName());
        if (dryRun) {
            LOG.info("Dropping table: {}.{}", table.getDb(), table.getName());
        } else {
            hms.dropTable(table.getDb(), table.getName(), false, true);
        }
    }

    private static List<PartitionInfo> queryPartitions(HMSClient hms, String dbName, String tableName) throws TException {
        LOG.trace("Getting partitions: {}.{}", dbName, tableName);
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
                            .map(p -> new PartitionInfo(FileUtils.makePartName(partColumns, p.getValues()), p))
                            .collect(Collectors.toList());
                    synchronized (al) {
                        al.addAll(pList);
                    }
                }
                return null;
            });
        }
        awaitTermination(pool, "Waiting for partitions to be received");
        return al;
    }

    public static void createPartitions(HMSClient hms, TableInfo table, List<PartitionInfo> parts) {
        if (parts.isEmpty()) {
            return;
        }
        LOG.trace("Creating partitions: {}.{}", table.getDb(), table.getName());
        int batchSize = 1000;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        for (int i = 0; i < parts.size(); i += batchSize) {
            List<PartitionInfo> batch = parts.subList(i, Math.min(i + batchSize, parts.size()));
            pool.submit(() -> {
                List<Partition> list = batch.stream()
                        .map(p -> makePartition(table.getTable(), p.getValues(), p.getLocation()))
                        .collect(Collectors.toList());
                try (HMSClient hmsClient = hms.createClient()) {
                    if (dryRun) {
                        LOG.info("Creating partitions: {}.{} {}", table.getDb(), table.getName(),
                                batch.stream().map(PartitionInfo::getName).collect(Collectors.joining(",")));
                    } else {
                        hmsClient.add_partitions(list, true, false);
                    }
                }
                return null;
            });
        }
        awaitTermination(pool, "Waiting for partitions to be created");
    }

    public static void dropPartitions(HMSClient hms, TableInfo table, List<PartitionInfo> parts) {
        if (parts.isEmpty()) {
            return;
        }
        LOG.trace("Dropping partitions: {}.{}", table.getDb(), table.getName());
        int batchSize = 1000;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        PartitionDropOptions options = new PartitionDropOptions()
                .deleteData(false)
                .ifExists(true)
                .returnResults(false);
        for (int i = 0; i < parts.size(); i += batchSize) {
            List<PartitionInfo> batch = parts.subList(i, Math.min(i + batchSize, parts.size()));
            pool.submit(() -> {
                try (HMSClient hmsClient = hms.createClient()) {
                    if (dryRun) {
                        LOG.info("Dropping partitions: {}.{} {}", table.getDb(), table.getName(),
                                batch.stream().map(PartitionInfo::getName).collect(Collectors.joining(",")));
                    } else {
                        for (PartitionInfo p : batch) {
                            hmsClient.dropPartition(table.getDb(), table.getName(), p.getValues(), options);
                        }
                    }
                }
                return null;
            });
        }
        awaitTermination(pool, "Waiting for partitions to be dropped");
    }

    private static Partition makePartition(Table table, List<String> values, String location) {
        Partition partition = new Partition();
        partition.setDbName(table.getDbName());
        partition.setTableName(table.getTableName());
        partition.setValues(values);
        partition.setLastAccessTime(0);

        StorageDescriptor sdCopy = table.getSd().deepCopy();
        sdCopy.setLocation(location);
        partition.setSd(sdCopy);

        Map<String, String> params = new HashMap<>();
        params.put("replicated", "true");
        partition.setParameters(params);

        return partition;
    }
}
