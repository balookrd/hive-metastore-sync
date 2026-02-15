package com.wandisco.hivesync.main;

import com.wandisco.hivesync.common.Tools;
import com.wandisco.hivesync.hive.Commands;
import com.wandisco.hivesync.hive.HMSClient;
import com.wandisco.hivesync.hive.PartitionInfo;
import com.wandisco.hivesync.hive.TableInfo;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.wandisco.hivesync.common.Tools.awaitTermination;

public class HiveSync {

    private static final Logger LOG = LogManager.getLogger(HiveSync.class);

    private final HMSClient srcHms;
    private final HMSClient dstHms;
    private final List<String> dbWildcards;
    private final List<String> tblWildcards;

    public HiveSync(String srcMeta, String dstMeta,
                    boolean metaSasl,
                    List<String> databases,
                    List<String> tables) throws Exception {
        srcHms = Tools.createNewMetaConnection(srcMeta, metaSasl);
        dstHms = Tools.createNewMetaConnection(dstMeta, metaSasl);
        this.dbWildcards = databases;
        this.tblWildcards = tables;
    }

    public void execute() throws Exception {
        HashSet<String> dbList1 = new HashSet<>();
        for (String database : dbWildcards) {
            dbList1.addAll(Commands.getDatabases(srcHms, database));
        }
        HashSet<String> dbList2 = new HashSet<>();
        for (String database : dbWildcards) {
            dbList2.addAll(Commands.getDatabases(dstHms, database));
        }

        ExecutorService pool = Executors.newFixedThreadPool(8);
        for (String db : dbList1) {
            LOG.info("Syncing database: {}", db);
            if (!dbList2.contains(db)) {
                Database db1 = srcHms.getDatabase(db);
                createDatabase(dstHms, db1);
            }
            pool.submit(() -> syncDatabase(db));
        }
        awaitTermination(pool, "Waiting for syncing databases");
    }

    private void createDatabase(HMSClient hms, Database db) throws Exception {
        LOG.info("Create database: {}", db.getName());
        Commands.createDatabase(hms, db);
    }

    private void syncDatabase(String database) {
        LOG.trace("Collect table information: {}", database);
        try (HMSClient srcHms = this.srcHms.createClient();
             HMSClient dstHms = this.dstHms.createClient()) {
            Map<String, TableInfo> srcTables = getTablesMap(Commands.getTables(srcHms, database));
            Map<String, TableInfo> dstTables = getTablesMap(Commands.getTables(dstHms, database));
            // update partitions for both tables
            Set<String> bothTables = new HashSet<>(srcTables.keySet());
            bothTables.retainAll(dstTables.keySet());
            if (!bothTables.isEmpty()) {
                syncPartitions(srcHms, dstHms, srcTables, dstTables, bothTables);
            }
            // create new (and remove old) tables from src in dst
            if (srcTables.size() != bothTables.size()) {
                syncTables(srcHms, dstHms, srcTables, bothTables);
            }
            // create new (and remove old) tables from dst in src
            if (dstTables.size() != bothTables.size()) {
                syncTables(dstHms, srcHms, dstTables, bothTables);
            }
        } catch (Exception e) {
            LOG.error("Error syncing database: {}", database, e);
        }
    }

    private void syncTables(HMSClient srcHms, HMSClient dstHms,
                            Map<String, TableInfo> tables,
                            Collection<String> bothTables) throws TException {
        List<TableInfo> newTables = new ArrayList<>();
        List<TableInfo> delTables = new ArrayList<>();
        for (Map.Entry<String, TableInfo> e : tables.entrySet()) {
            if (!bothTables.contains(e.getKey())) {
                if (e.getValue().isReplicated()) {
                    delTables.add(e.getValue());
                } else {
                    newTables.add(e.getValue());
                }
            }
        }
        for (TableInfo ti : newTables) {
            LOG.info("Create non-existing table: {}.{}", ti.getDb(), ti.getName());
            Commands.createTable(dstHms, ti);
        }
        for (TableInfo ti : delTables) {
            LOG.info("Drop replicated table: {}.{}", ti.getDb(), ti.getName());
            Commands.dropTable(srcHms, ti);
        }
    }

    private void syncPartitions(HMSClient srcHms, HMSClient dstHms,
                                Map<String, TableInfo> srcTables, Map<String, TableInfo> dstTables,
                                Collection<String> bothTables) {
        for (String table : bothTables) {
            TableInfo srcTable = srcTables.get(table);
            if (srcTable.isPartitioned()) {
                LOG.info("Update partitions of existing table: {}.{}", srcTable.getDb(), srcTable.getName());
                TableInfo dstTable = dstTables.get(table);
                Map<String, PartitionInfo> srcParts = getPartitionsMap(srcTable);
                Map<String, PartitionInfo> dstParts = getPartitionsMap(dstTable);
                // update partitions for both tables
                Set<String> bothParts = new HashSet<>(srcParts.keySet());
                bothParts.retainAll(dstParts.keySet());
                // create new partitions from src in dst
                if (srcParts.size() != bothParts.size()) {
                    syncTablePartitions(srcHms, dstHms, srcTable, srcParts, bothParts);
                }
                // create new partitions from dst in src
                if (dstParts.size() != bothParts.size()) {
                    syncTablePartitions(dstHms, srcHms, dstTable, dstParts, bothParts);
                }
            }
        }
    }

    private void syncTablePartitions(HMSClient srcHms, HMSClient dstHms, TableInfo table,
                                     Map<String, PartitionInfo> parts, Collection<String> bothParts) {
        List<PartitionInfo> newParts = new ArrayList<>();
        List<PartitionInfo> delParts = new ArrayList<>();
        for (Map.Entry<String, PartitionInfo> e : parts.entrySet()) {
            if (!bothParts.contains(e.getKey())) {
                if (e.getValue().isReplicated()) {
                    delParts.add(e.getValue());
                } else {
                    newParts.add(e.getValue());
                }
            }
        }
        Commands.createPartitions(dstHms, table, newParts);
        Commands.dropPartitions(srcHms, table, delParts);
    }

    private Map<String, TableInfo> getTablesMap(Collection<TableInfo> tables) {
        return tables.stream()
                .filter(t -> t.nonTransactional() && Tools.match(tblWildcards, t.getName()))
                .collect(Collectors.toMap(TableInfo::getName, t -> t));
    }

    private Map<String, PartitionInfo> getPartitionsMap(TableInfo table) {
        return table.getPartitions().stream()
                .collect(Collectors.toMap(PartitionInfo::getName, t -> t));
    }
}
