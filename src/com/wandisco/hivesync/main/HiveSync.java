package com.wandisco.hivesync.main;

import com.wandisco.hivesync.common.Tools;
import com.wandisco.hivesync.hive.Commands;
import com.wandisco.hivesync.hive.HMSClient;
import com.wandisco.hivesync.hive.TableInfo;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 *
 * @author Oleg Danilov
 *
 */
public class HiveSync {

    private final HMSClient hms1;
    private final HMSClient hms2;
    private final List<String> dbWildcards;
    private final List<String> tblWildcards;

    private static final Logger LOG = LogManager.getLogger(HiveSync.class);

    public HiveSync(String srcMeta, String dstMeta,
                    boolean metaSasl,
                    List<String> databases,
                    List<String> tables) throws Exception {
        hms1 = Tools.createNewMetaConnection(srcMeta, metaSasl);
        hms2 = Tools.createNewMetaConnection(dstMeta, metaSasl);
        this.dbWildcards = databases;
        this.tblWildcards = tables;
    }

    public void execute() throws Exception {
        HashSet<String> dbList1 = new HashSet<>();
        for (String database : dbWildcards) {
            dbList1.addAll(Commands.getDatabases(hms1, database));
        }
        HashSet<String> dbList2 = new HashSet<>();
        for (String database : dbWildcards) {
            dbList2.addAll(Commands.getDatabases(hms2, database));
        }

        ExecutorService pool = Executors.newFixedThreadPool(8);
        for (String db : dbList1) {
            LOG.info("Syncing database {}", db);
            if (!dbList2.contains(db)) {
                Database db1 = hms1.getDatabase(db);
                createDatabase(hms2, db1);
            }
            pool.submit(() -> syncDatabase(db));
        }
        Commands.awaitTermination(pool, "Waiting for syncing databases");
    }

    private void createDatabase(HMSClient hms, Database db) throws Exception {
        LOG.info("Create database {}", db.getName());
        Commands.createDatabase(hms, db);
    }

    private void syncDatabase(String database) {
        LOG.trace("Collect table information {}", database);
        try (HMSClient hms1 = this.hms1.createClient();
             HMSClient hms2 = this.hms2.createClient();) {
            List<TableInfo> srcTables = Commands.getTables(hms1, database).stream()
                    .filter(t -> t.nonTransactional() && Tools.match(tblWildcards, t.getName()))
                    .collect(Collectors.toList());
            List<TableInfo> dstTables = Commands.getTables(hms2, database).stream()
                    .filter(t -> t.nonTransactional() && Tools.match(tblWildcards, t.getName()))
                    .collect(Collectors.toList());
            for (TableInfo srcTable : srcTables) {
                TableInfo dstTable = findTable(dstTables, srcTable.getName());
                if (dstTable == null) {
                    LOG.info("Create non-existing table {}", srcTable.getName());
                    Commands.createTable(hms2, srcTable);
                } else {
                    LOG.info("Update existing table {}", dstTable.getName());
                    Commands.updatePartitions(hms2, srcTable, dstTable);
                }
            }
            for (TableInfo dstTable : dstTables) {
                if (findTable(srcTables, dstTable.getName()) == null) {
                    LOG.info("Drop table {}", dstTable.getName());
                    Commands.dropTable(hms2, dstTable);
                }
            }
        } catch (Exception e) {
            LOG.error("Error syncing database {}", database, e);
        }
    }

    private TableInfo findTable(Collection<TableInfo> tables, String tableName) {
        for (TableInfo ti : tables) {
            if (ti.getName().equalsIgnoreCase(tableName)) {
                return ti;
            }
        }
        return null;
    }
}
