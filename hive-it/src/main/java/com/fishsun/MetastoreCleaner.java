package com.fishsun;

import java.nio.file.Files;
import java.nio.file.Paths;

public class MetastoreCleaner {
    public static void cleanMetastore() throws Exception {
        Files.deleteIfExists(Paths.get("metastore_db"));
        Files.deleteIfExists(Paths.get("warehouse"));
    }
}
