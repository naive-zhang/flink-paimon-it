package com.fishsun.bigdata.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public class FileUtils {
    public static String getDefaultLakeHousePath() {
        Path path = Paths.get("");
        Path lakehouseDefaultPath = Paths.get(path.toAbsolutePath() + "/./lakehosue");
        return lakehouseDefaultPath
                .normalize()
                .toAbsolutePath().toString();
    }

    public static void clearLakehouse() throws IOException {
        String lakeHousePath = getDefaultLakeHousePath();
        deleteDirectoryAndContents(Paths.get(lakeHousePath));
    }

    // 静态方法用于删除目录及其下所有文件和目录
    private static void deleteDirectoryAndContents(Path path) throws IOException {
        File file = path.toFile();
        if (!file.exists()) {
            return;
        } else if (file.isFile()) {
            file.delete();
        }
        // 使用 walkFileTree 递归删除文件
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);  // 删除文件
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);  // 删除目录
                return FileVisitResult.CONTINUE;
            }
        });
    }


}
