package com.daoke360.Dao;

import com.daoke360.common.DateTypeEnum;
import com.daoke360.domain.DateDimension;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by ♠K on 2017-8-1.
 * QQ:272488352
 */
public class DateDimensionDao$Test {
    @Test
    public void findId() throws Exception {
        DateDimension dateDimension = DateDimension.buildDate(System.currentTimeMillis(), DateTypeEnum.day());

        System.out.println(DateDimensionDao.findId(dateDimension));

    }

}