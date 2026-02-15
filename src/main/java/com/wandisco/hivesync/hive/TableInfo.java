package com.wandisco.hivesync.hive;

import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

import static com.wandisco.hivesync.common.Tools.getBoolParameter;

public class TableInfo {

    private final Table table;
    private final List<PartitionInfo> partitions;

    private final boolean isManaged;
    private final boolean isTransactional;
    private final boolean isPartitioned;
    private final boolean isReplicated;

    public TableInfo(Table table, List<PartitionInfo> partitions) {
        this.table = table;
        this.partitions = partitions;
        this.isManaged = table.getTableType().equalsIgnoreCase("MANAGED_TABLE");
        this.isTransactional = getBoolParameter(table.getParameters(), "transactional");
        this.isPartitioned = table.getPartitionKeysSize() != 0;
        this.isReplicated = getBoolParameter(table.getParameters(), "replicated");
    }

    public Table getTable() {
        return table;
    }

    public String getDb() {
        return table.getDbName();
    }

    public String getName() {
        return table.getTableName();
    }

    public List<PartitionInfo> getPartitions() {
        return partitions;
    }

    public boolean isManaged() {
        return isManaged;
    }

    public boolean nonTransactional() {
        return !isTransactional;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public boolean isReplicated() {
        return isReplicated;
    }

    @Override
    public String toString() {
        return "Table: " + getTable() + " Managed: " + isManaged + " Transactional:" + isTransactional;
    }
}
