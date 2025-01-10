package com.fishsun.bigdata.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class IncomeInfo implements Serializable {
    protected static final List<String> nameLists = Arrays.asList(
            "张三", "李四", "王五", "赵六", "孙七", "周八", "吴九", "郑十", "王大", "陈二", "刘三", "杨四"
    );
    protected static final List<String> genderList = Arrays.asList("男", "女", "未知");
    protected static final List<String> deptList = Arrays.asList(
            "IT", "财务", "运营", "HRBP", "市场", "销售", "客服", "物流", "研发", "行政", "法务", "采购"
    );
    public String name;
    public String gender;
    public String dept;
    public Double income;


    public static String pickupName() {
        return nameLists.get((int) (Math.random() * nameLists.size()));
    }

    public static String pickupGender() {
        return genderList.get((int) (Math.random() * genderList.size()));
    }

    public static String pickupDept() {
        return deptList.get((int) (Math.random() * deptList.size()));
    }
}
