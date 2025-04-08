package com.fishsun.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * @Author: zhangxinsen
 * @Date: 2025/4/7 12:36
 * @Desc:
 * @Version: v1.0
 */

public class TimeUtils {
    /**
     * 计算从开始时间到结束时间按 intervalTime 分钟分割的时间段数量
     *
     * @param startTimeStr 开始时间字符串，格式为 "YYYY-mm-dd" 或 "YYYY-mm-dd HH:MM:SS"
     * @param endTimeStr   结束时间字符串，格式同上
     * @param intervalTime 时间间隔（分钟数）
     * @return 完整时间段数量
     */
    public static int calculateSegments(String startTimeStr, String endTimeStr, int intervalTime) {
        // 检查 intervalTime 是否有效
        if (intervalTime <= 0) {
            throw new IllegalArgumentException("intervalTime 必须大于 0");
        }

        // 统一时间格式
        startTimeStr = normalizeTimeString(startTimeStr);
        endTimeStr = normalizeTimeString(endTimeStr);

        // 定义时间格式化器
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 解析时间字符串为 LocalDateTime
        LocalDateTime startTime = LocalDateTime.parse(startTimeStr, formatter);
        LocalDateTime endTime = LocalDateTime.parse(endTimeStr, formatter);

        // 检查开始时间是否晚于结束时间
        if (startTime.isAfter(endTime)) {
            throw new IllegalArgumentException("开始时间必须早于结束时间");
        }

        // 计算总分钟数
        long totalMinutes = ChronoUnit.MINUTES.between(startTime, endTime);

        // 计算完整时间段数量（向下取整）
        int segments = (int) (totalMinutes / intervalTime);
        if ((long) segments * intervalTime < totalMinutes) {
            segments++;
        }

        return segments;
    }

    /**
     * 规范化时间字符串，如果是 "YYYY-mm-dd" 格式，补齐为 "YYYY-mm-dd 00:00:00"
     *
     * @param timeStr 输入时间字符串
     * @return 规范化后的时间字符串
     */
    private static String normalizeTimeString(String timeStr) {
        if (timeStr.length() == 10) { // "YYYY-mm-dd" 格式
            return timeStr + " 00:00:00";
        } else if (timeStr.length() == 19) { // "YYYY-mm-dd HH:MM:SS" 格式
            return timeStr;
        } else {
            throw new IllegalArgumentException("时间格式无效: " + timeStr);
        }
    }

    public static void main(String[] args) {
        System.out.println(calculateSegments("2025-03-01", "2025-03-02", 1));
        System.out.println(calculateSegments("2025-03-01", "2025-03-02", 3));
        System.out.println(calculateSegments("2025-03-01", "2025-03-02", 5));
        System.out.println(calculateSegments("2025-03-01", "2025-03-02", 7));
    }
}
