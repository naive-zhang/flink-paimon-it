package com.fishsun.bigdata.utils;

import java.io.File;
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

    public static String getPipelineIOCachePath() {
        return getPipelineIOCachePath(true);
    }

    public static String getPipelineIOCachePath(boolean normalize) {
        Path path = Paths.get("");
        String pipePath = path.toAbsolutePath().toString() + "/tmp";
        return normalize ? normalize(Paths.get(pipePath).normalize().toString()) :
                Paths.get(pipePath).normalize().toString();
    }

    public static String getLakehouseDefaultPath(boolean normalized) {
        Path path = Paths.get("");
        String lakehousePath = path.toAbsolutePath() + "/lakehouse";
        return normalized ? normalize(Paths.get(lakehousePath).normalize().toString()) :
                Paths.get(lakehousePath).normalize().toString();
    }

    public static String getCheckpointPath(boolean normalized) {
        Path path = Paths.get("");
        String ckPath = path.toAbsolutePath() + "/checkpoint";
        return normalized ? normalize(Paths.get(ckPath).normalize().toString()) :
                Paths.get(ckPath).normalize().toString();
    }

    public static String getLakehouseDefaultPath() {
        return getLakehouseDefaultPath(true);
    }

    public static String getCheckpointPath() {
        return getCheckpointPath(true);
    }

    public static void clearDir(String dir, boolean ensureDirExists) {
        Path path = Paths.get(dir);
        if (path.toFile().exists() && path.toFile().isDirectory()) {
            deleteDirectory(path.toFile());
        }
        path = Paths.get(dir);
        if (path == null || !path.toFile().exists() && ensureDirExists) {
            path.toFile().mkdirs();
        }
    }

    public static void clearDir(String dir) {
        clearDir(dir, false);
    }

    private static boolean deleteDirectory(File directory) {
        // 如果目录为空，直接删除
        if (directory == null || !directory.exists()) {
            return false;
        }

        // 判断是否是目录
        if (directory.isDirectory()) {
            // 获取目录下的所有文件和子目录
            File[] files = directory.listFiles();
            if (files != null) {
                // 遍历目录下的文件和子目录，递归删除
                for (File file : files) {
                    // 递归删除子文件/子目录
                    deleteDirectory(file);
                }
            }
        }
        // 删除文件或目录本身
        return directory.delete();
    }
}
