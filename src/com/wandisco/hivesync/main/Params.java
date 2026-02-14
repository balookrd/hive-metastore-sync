package com.wandisco.hivesync.main;

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.Collections;
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

    @Parameter(names = {"--src-meta"},
            description = "Thrift connection string to metastore thrift://host:port",
            required = true)
    private String srcMeta;

    @Parameter(names = {"--dst-meta"},
            description = "Thrift connection string to metastore thrift://host:port",
            required = true)
    private String dstMeta;

    @Parameter(names = {"--meta-sasl"},
            description = "hive.metastore.sasl.enabled = true",
            required = false)
    private Boolean metaSasl = false;

    @Parameter(names = {"--database"},
            description = "Database(s), comma-separated list with wildcards")
    private List<String> databases = new ArrayList<>(Collections.singletonList("default"));

    @Parameter(names = {"--table"},
            description = "Table(s), comma-separated list with wildcards")
    private List<String> tables = new ArrayList<>(Collections.singletonList("*"));

    @Parameter(names = {"--dry-run"},
            description = "Don't run, but output commands to the specified file")
    private String dryRunFile = null;

    public Boolean getHelp() {
        return help;
    }

    public String getSrcMeta() {
        return srcMeta;
    }

    public String getDstMeta() {
        return dstMeta;
    }

    public boolean isMetaSasl() {
        return metaSasl != null && metaSasl;
    }

    public List<String> getDatabases() {
        return databases;
    }

    public List<String> getTables() {
        return tables;
    }

    public String getDryRunFile() {
        return dryRunFile;
    }
}
