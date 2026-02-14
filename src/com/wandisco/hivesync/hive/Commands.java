package com.wandisco.hivesync.hive;

import com.wandisco.hivesync.common.Tools;
import com.wandisco.hivesync.main.HiveSync;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class Commands {

    private static final Logger LOG = LogManager.getLogger(HiveSync.class);

    private static String dryRunFile = null;

    public static void setDryRunFile(String name) {
        dryRunFile = name;
    }

    public static List<String> getDatabases(HMSClient hms, String pattern) throws TException {
        LOG.trace("Getting database list");
        return hms.getAllDatabases().stream()
                .filter(d -> Tools.match(pattern, d))
                .collect(Collectors.toList());
    }

    public static void createDatabase(HMSClient hms, Database db) throws TException {
        LOG.trace("Creating database {}", db.getName());
        Database dbCopy = db.deepCopy();
        dbCopy.unsetParameters();
        hms.createDatabase(dbCopy);
    }

    public static ArrayList<TableInfo> getTables(HMSClient hms, String dbName) throws TException {
        LOG.trace("Getting table list from {}", dbName);
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
        LOG.trace("Creating table {}.{}", table.getDb(), table.getName());
        Table tableCopy = table.getTable().deepCopy();
        tableCopy.setTableType("EXTERNAL_TABLE");
        Map<String, String> params = new HashMap<>();
        params.put("EXTERNAL", "TRUE");
        tableCopy.setParameters(params);
        hms.createTable(tableCopy);
        createPartitions(hms, table, table.getPartitions());
    }

    public static void dropTable(HMSClient hms, TableInfo table) throws Exception {
        LOG.trace("Dropping table {}.{}", table.getDb(), table.getName());
        hms.dropTable(table.getDb(), table.getName(), false, true);
    }

    public static void updatePartitions(HMSClient hms, TableInfo src, TableInfo dst) {
        LOG.trace("Updating partitions {}.{}", dst.getDb(), dst.getName());
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

    private static List<PartitionInfo> queryPartitions(HMSClient hms, String dbName, String tableName) throws TException {
        LOG.trace("Getting partitions {}.{}", dbName, tableName);
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
                            .map(p ->
                                    new PartitionInfo(FileUtils.makePartName(partColumns, p.getValues()),
                                            p.getValues(), p.getSd().getLocation())
                            )
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

    private static void createPartitions(HMSClient hms, TableInfo table, List<PartitionInfo> parts) {
        LOG.trace("Creating partitions {}.{}", table.getDb(), table.getName());
        int batchSize = 1000;
        ExecutorService pool = Executors.newFixedThreadPool(6);
        for (int i = 0; i < parts.size(); i += batchSize) {
            List<PartitionInfo> batch = parts.subList(i, Math.min(i + batchSize, parts.size()));
            pool.submit(() -> {
                List<Partition> list = batch.stream()
                        .map(p -> createPartition(table.getTable(), p.getValues(), p.getLocation()))
                        .collect(Collectors.toList());
                try (HMSClient hmsClient = hms.createClient()) {
                    hmsClient.add_partitions(list, true, false);
                }
                return null;
            });
        }
        awaitTermination(pool, "Waiting for partitions to be created");
    }

    private static void dropPartitions(HMSClient hms, TableInfo table, List<PartitionInfo> parts) {
        LOG.trace("Dropping partitions {}.{}", table.getDb(), table.getName());
        int batchSize = 1000;
        ExecutorService pool = Executors.newFixedThreadPool(6);
        PartitionDropOptions options = new PartitionDropOptions()
                .deleteData(false)
                .ifExists(true)
                .returnResults(false);
        for (int i = 0; i < parts.size(); i += batchSize) {
            List<PartitionInfo> batch = parts.subList(i, Math.min(i + batchSize, parts.size()));
            pool.submit(() -> {
                try (HMSClient hmsClient = hms.createClient()) {
                    for (PartitionInfo p : batch) {
                        hmsClient.dropPartition(table.getDb(), table.getName(), p.getValues(), options);
                    }
                }
                return null;
            });
        }
        awaitTermination(pool, "Waiting for partitions to be dropped");
    }

    private static PartitionInfo findPartition(List<PartitionInfo> parts, String partition) {
        for (PartitionInfo pi : parts) {
            if (pi.getName().equalsIgnoreCase(partition)) {
                return pi;
            }
        }
        return null;
    }

    private static Partition createPartition(Table table, List<String> values, String location) {
        StorageDescriptor sdCopy = table.getSd().deepCopy();
        sdCopy.setLocation(location);
        Partition partition = new Partition();
        partition.setDbName(table.getDbName());
        partition.setTableName(table.getTableName());
        partition.setValues(values);
        partition.setSd(sdCopy);
        partition.setCreateTime((int) (System.currentTimeMillis() / 1000));
        partition.setLastAccessTime(0);
        partition.unsetParameters();
        return partition;
    }

    public static void awaitTermination(ExecutorService pool, String text) {
        pool.shutdown();
        try {
            while (!pool.awaitTermination(1, TimeUnit.SECONDS)) {
                LOG.trace(text);
            }
        } catch (InterruptedException ignored) {
        }
    }
}
