package com.fishsun.bigdata.utils;

import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtils {
    public static String getLakehouseDefaultPath() {
        Path path = Paths.get("");
        String lakehousePath = path.toAbsolutePath() + "/lakehouse";
        return Paths.get(lakehousePath).normalize().toString();
    }
}
