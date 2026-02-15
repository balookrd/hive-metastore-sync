package com.wandisco.hivesync.main;

import com.wandisco.hivesync.common.Tools;
import com.wandisco.hivesync.hive.Commands;
import com.wandisco.hivesync.hive.HMSClient;
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

/**
 *
 * @author Oleg Danilov
 *
 */
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
            Map<String, TableInfo> srcTables = filterTables(Commands.getTables(srcHms, database));
            Map<String, TableInfo> dstTables = filterTables(Commands.getTables(dstHms, database));
            // update partitions for both tables
            Set<String> bothTables = new HashSet<>(srcTables.keySet());
            bothTables.retainAll(dstTables.keySet());
            if (!bothTables.isEmpty()) {
                updateTablesPartitions(srcHms, dstHms, srcTables, dstTables, bothTables);
            }
            // create new tables from src in dst
            if (srcTables.size() != bothTables.size()) {
                updateTables(srcHms, dstHms, srcTables, bothTables);
            }
            // create new tables from dst in src
            if (dstTables.size() != bothTables.size()) {
                updateTables(dstHms, srcHms, dstTables, bothTables);
            }
        } catch (Exception e) {
            LOG.error("Error syncing database: {}", database, e);
        }
    }

    private Map<String, TableInfo> filterTables(Collection<TableInfo> tables) {
        return tables.stream()
                .filter(t -> t.nonTransactional() && Tools.match(tblWildcards, t.getName()))
                .collect(Collectors.toMap(TableInfo::getName, t -> t));
    }

    private void updateTables(HMSClient srcHms, HMSClient dstHms,
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

    private void updateTablesPartitions(HMSClient srcHms, HMSClient dstHms,
                                        Map<String, TableInfo> srcTables, Map<String, TableInfo> dstTables,
                                        Collection<String> bothTables) {
        for (String table : bothTables) {
            LOG.info("Update existing table: {}", table);
            Commands.updatePartitions(srcHms, dstHms, srcTables.get(table), dstTables.get(table));
        }
    }
}
