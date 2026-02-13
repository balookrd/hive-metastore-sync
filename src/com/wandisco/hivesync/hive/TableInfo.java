package com.wandisco.hivesync.hive;

import com.wandisco.hivesync.common.Tools;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.stream.Collectors;

public class TableInfo {

    private final Table table;
    private final String name;
    private final String createCommand;
    private final boolean isManaged;
    private final boolean isTransactional;
    private final List<PartitionInfo> partitions;

    public TableInfo(Table table, String name, List<String> createCommand, List<PartitionInfo> partitions, boolean isManaged) {
        this.table = table;
        this.name = name;
        this.createCommand = Tools.join(createCommand.stream()
                .filter(s -> !s.contains("transactional") && !s.contains("external.table.purge"))
                .map(s -> (s.startsWith("CREATE TABLE ") ? "CREATE EXTERNAL TABLE " + s.substring(13) : s))
                .collect(Collectors.toList()));
        this.partitions = partitions;
        this.isManaged = isManaged;
        //table.getParameters().get("transactional");
        this.isTransactional = createCommand.stream().anyMatch(s -> s.contains("'transactional'='true'"));
    }

    public Table getTable() {
        return table;
    }

    public String getName() {
        return name;
    }

    public String getCreateCommand() {
        return createCommand;
    }

    public boolean isManaged() {
        return isManaged;
    }

    public boolean isTransactional() {
        return isTransactional;
    }

    @Override
    public String toString() {
        return "Table: " + name + " Managed: " + isManaged + " Transactional:" + isTransactional + " Create Command: " + createCommand;
    }

    public List<PartitionInfo> getPartitions() {
        return partitions;
    }
}
