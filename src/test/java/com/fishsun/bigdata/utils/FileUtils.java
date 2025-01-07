package com.fishsun.bigdata.utils;

import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtils {

    private static String normalize(String path) {
        path = path.replace("\\", "/");
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return path;
    }

    public static String getLakehouseDefaultPath() {
        Path path = Paths.get("");
        String lakehousePath = path.toAbsolutePath() + "/lakehouse";
        return normalize(Paths.get(lakehousePath).normalize().toString());
    }

    public static String getCheckpointPath() {
        Path path = Paths.get("");
        String ckPath = path.toAbsolutePath() + "/checkpoint";
        return normalize(Paths.get(ckPath).normalize().toString());
    }
}
