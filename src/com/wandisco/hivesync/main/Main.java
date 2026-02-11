package com.wandisco.hivesync.main;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.wandisco.hivesync.hive.Commands;

import java.io.File;

/**
 *
 * @author Oleg Danilov
 *
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Params p = new Params();
        JCommander jce = new JCommander(p);
        try {
            jce.parse(args);
        } catch (ParameterException e) {
            System.err.println("ERROR: " + e.getMessage());
            jce.usage();
            System.exit(1);
        }

        if (p.getHelp() != null) {
            jce.setProgramName("hive-metastore-sync");
            jce.usage();
            return;
        }

        // Delete dryRun file
        String dryRunFile = p.getDryRunFile();
        Commands.setDryRunFile(dryRunFile);
        if (dryRunFile != null) {
            (new File(dryRunFile)).delete();
        }

        HiveSync hs = new HiveSync(p.getSrc(), p.getDst(), p.getDatabases());
        hs.execute();
    }
}
