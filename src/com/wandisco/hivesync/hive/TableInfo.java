package com.wandisco.hivesync.hive;

import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

public class TableInfo {

    private final Table table;
    private final List<PartitionInfo> partitions;

    private final boolean isManaged;
    private final boolean isTransactional;

    public TableInfo(Table table, List<PartitionInfo> partitions) {
        this.table = table;
        this.partitions = partitions;
        this.isManaged = table.getTableType().equalsIgnoreCase("MANAGED_TABLE");
        String transactional = table.getParameters().get("transactional");
        this.isTransactional = transactional != null && transactional.equalsIgnoreCase("true");
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

    @Override
    public String toString() {
        return "Table: " + getTable() + " Managed: " + isManaged + " Transactional:" + isTransactional;
    }
}
