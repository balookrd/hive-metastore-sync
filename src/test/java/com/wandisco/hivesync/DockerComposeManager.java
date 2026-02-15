package com.wandisco.hivesync;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class DockerComposeManager {

    private static final Map<String, Process> composeProcess = new HashMap<>();

    public static void start(String file) throws IOException, InterruptedException {
        if (!Files.exists(Paths.get(file))) {
            throw new RuntimeException("Can't open docker compose file " + file);
        }
        ProcessBuilder pb = new ProcessBuilder(
                "docker",
                "compose",
                "-f", file,
                "up", "-d"
        );
        pb.redirectErrorStream(true);
        Process p = pb.start();
        composeProcess.put(file, p);
        p.waitFor();
    }

    public static void stop(String file) throws IOException, InterruptedException {
        if (!Files.exists(Paths.get(file))) {
            throw new RuntimeException("Can't open docker compose file " + file);
        }
        Process p = composeProcess.remove(file);
        if (p != null && p.isAlive()) {
            p.destroy();
        }
        ProcessBuilder pb = new ProcessBuilder(
                "docker",
                "compose",
                "-f", file,
                "down"
        );
        pb.start().waitFor();
    }
}
