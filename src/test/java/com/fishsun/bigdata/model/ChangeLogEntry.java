package com.fishsun.bigdata.model;

public class ChangeLogEntry<T> {
    public ChangeLogType type;  // 变更类型
    public T entry; // 数据
}
