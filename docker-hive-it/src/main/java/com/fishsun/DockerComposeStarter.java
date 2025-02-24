package com.fishsun;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class DockerComposeStarter {
    public static void startDockerCompose() throws Exception {
        ProcessBuilder pb = new ProcessBuilder("docker-compose", "up", "-d");
        pb.directory(new File(getDockerComposeTmpDirectory())); // 替换为实际路径
        Process process = pb.start();
        process.waitFor();
        System.out.println("Docker Compose started successfully.");
        Thread.sleep(Long.MAX_VALUE);

    }


    /**
     * 将resources/docker-compose.yml创建到一个临时文件中
     *
     * @return
     * @throws Exception
     */
    private static String getDockerComposeTmpDirectory() throws Exception {
        // 创建临时目录，例如 /tmp/docker-compose4282026977885520073
        Path tempDir = Files.createTempDirectory("docker-compose");

        // 获取 docker-compose.yml 的资源输入流
        try (InputStream inputStream = DockerComposeStarter.class.getClassLoader()
                .getResourceAsStream("docker-compose.yml")) {
            if (inputStream == null) {
                throw new IllegalStateException("无法找到 docker-compose.yml 文件");
            }

            // 在临时目录下创建 docker-compose.yml 文件
            Path dockerComposePath = tempDir.resolve("docker-compose.yml");

            // 将资源内容复制到该文件中
            Files.copy(inputStream, dockerComposePath, StandardCopyOption.REPLACE_EXISTING);

            // 返回临时目录的绝对路径
            return tempDir.toAbsolutePath().toString();
        }
    }

    public static void main(String[] args) throws Exception {
        startDockerCompose();
    }
}