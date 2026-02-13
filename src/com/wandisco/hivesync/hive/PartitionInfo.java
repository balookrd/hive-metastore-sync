package com.wandisco.hivesync.hive;

import java.util.List;

public class PartitionInfo {

    private final String name;
    private final List<String> values;
    private final String location;

    public PartitionInfo(String name, List<String> values, String location) {
        this.name = name;
        this.values = values;
        this.location = location;
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

    public String getNameTranslated() {
        return name.replaceAll("=", "='").replaceAll("/", "',") + "'";
    }
}
