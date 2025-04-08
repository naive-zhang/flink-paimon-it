package com.fishsun.conf;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Author: zhangxinsen
 * @Date: 2025/4/8 16:32
 * @Desc:
 * @Version: v1.0
 */

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class TaskConf {

    public static final String JDBC2PAIMON_TASK = "jdbc2paimon";
    public static final String HIVE2PAIMON_TASK = "hive2paimon";

    public static final String TASK_TYPE_KEY = "task_type";
    public static final List<String> TASK_TYPES = Arrays.asList(JDBC2PAIMON_TASK, HIVE2PAIMON_TASK);

    // 不可变的属性
    private String taskType;

    public static TaskConf toTaskConf(Map<String, String> taskParams) {
        if (taskParams == null) {
            throw new IllegalArgumentException("task params is null when toTaskConf");
        }
        TaskConfBuilder builder = TaskConf.builder();
        if (taskParams.containsKey(TASK_TYPE_KEY)) {
            if (TASK_TYPES.stream().anyMatch(x -> x.trim().equalsIgnoreCase(taskParams.get(TASK_TYPE_KEY)))) {
                builder.taskType(taskParams.get(TASK_TYPE_KEY));
            } else {
                throw new IllegalArgumentException("task params error, task type is " + taskParams.get(TASK_TYPE_KEY));
            }
        } else {
            throw new IllegalArgumentException("task params is null when toTaskConf");
        }
        return builder.build();
    }
}
