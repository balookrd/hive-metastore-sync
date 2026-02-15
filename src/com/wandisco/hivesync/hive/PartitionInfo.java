package com.wandisco.hivesync.hive;

import org.apache.hadoop.hive.metastore.api.Partition;

import java.util.List;

import static com.wandisco.hivesync.common.Tools.getBoolParameter;

public class PartitionInfo {

    private final String name;
    private final List<String> values;
    private final String location;
    private final boolean isReplicated;

    public PartitionInfo(String name, Partition partition) {
        this.name = name;
        this.values = partition.getValues();
        this.location = partition.getSd().getLocation();
        this.isReplicated = getBoolParameter(partition.getParameters(), "replicated");
    }

    public String getName() {
        return name;
    }

    public List<String> getValues() {
        return values;
    }

    public String getLocation() {
        return location;
    }

    public boolean isReplicated() {
        return isReplicated;
    }
}
