package com.fishsun.bigdata.model;

import org.junit.Assert;
import org.junit.Test;

public class IncomeInfoTestSuite {
    @Test
    public void testRandomPickupName() {
        Assert.assertTrue(IncomeInfo.nameLists.contains(IncomeInfo.pickupName()));
        Assert.assertTrue(IncomeInfo.nameLists.contains(IncomeInfo.pickupName()));
        Assert.assertTrue(IncomeInfo.nameLists.contains(IncomeInfo.pickupName()));
        Assert.assertTrue(IncomeInfo.nameLists.contains(IncomeInfo.pickupName()));
        Assert.assertTrue(IncomeInfo.nameLists.contains(IncomeInfo.pickupName()));
        Assert.assertTrue(IncomeInfo.nameLists.contains(IncomeInfo.pickupName()));
    }


    @Test
    public void testRandomPickupGender() {
        Assert.assertTrue(IncomeInfo.genderList.contains(IncomeInfo.pickupGender()));
        Assert.assertTrue(IncomeInfo.genderList.contains(IncomeInfo.pickupGender()));
        Assert.assertTrue(IncomeInfo.genderList.contains(IncomeInfo.pickupGender()));
        Assert.assertTrue(IncomeInfo.genderList.contains(IncomeInfo.pickupGender()));
        Assert.assertTrue(IncomeInfo.genderList.contains(IncomeInfo.pickupGender()));
        Assert.assertTrue(IncomeInfo.genderList.contains(IncomeInfo.pickupGender()));
    }


    @Test
    public void testRandomPickupDept() {
        Assert.assertTrue(IncomeInfo.deptList.contains(IncomeInfo.pickupDept()));
        Assert.assertTrue(IncomeInfo.deptList.contains(IncomeInfo.pickupDept()));
        Assert.assertTrue(IncomeInfo.deptList.contains(IncomeInfo.pickupDept()));
        Assert.assertTrue(IncomeInfo.deptList.contains(IncomeInfo.pickupDept()));
        Assert.assertTrue(IncomeInfo.deptList.contains(IncomeInfo.pickupDept()));
        Assert.assertTrue(IncomeInfo.deptList.contains(IncomeInfo.pickupDept()));
    }
}
