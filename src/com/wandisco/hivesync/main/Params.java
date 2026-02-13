package com.wandisco.hivesync.main;

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Command line parameters
 *
 * @author Oleg Danilov
 *
 */
public class Params {

    @Parameter(names = {"-?", "--help"}, description = "Show help", help = true)
    private Boolean help;

    @Parameter(names = {"--src-hive"},
            description = "JDBC connection string ('hiveserver' and 'hiveserver2' can be used as default connection string for the corresponding services)",
            required = true)
    private String srcHive;

    @Parameter(names = {"--src-meta"},
            description = "Thrift connection string to metastore thrift://host:port",
            required = true)
    private String srcMeta;

    @Parameter(names = {"--dst-hive"},
            description = "JDBC connection string ('hiveserver' and 'hiveserver2' can be used as default connection string for the corresponding services)",
            required = true)
    private String dstHive;

    @Parameter(names = {"--dst-meta"},
            description = "Thrift connection string to metastore thrift://host:port",
            required = true)
    private String dstMeta;

    @Parameter(names = {"--database"},
            description = "Database(s), comma-separated list with wildcards")
    private List<String> databases = new ArrayList<>(Arrays.asList("default"));

    @Parameter(names = {"-n", "--dry-run"},
            description = "Don't run, but output commands to the specified file")
    private String dryRunFile = null;

    public Boolean getHelp() {
        return help;
    }

    public String getSrcHive() {
        return srcHive;
    }

    public String getSrcMeta() {
        return srcMeta;
    }

    public String getDstHive() {
        return dstHive;
    }

    public String getDstMeta() {
        return dstMeta;
    }

    public List<String> getDatabases() {
        return databases;
    }

    public String getDryRunFile() {
        return dryRunFile;
    }

}
