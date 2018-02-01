package com.daoke360.task.spark_streaming.convert;

import com.daoke360.domain.LocationDimension;
import com.daoke360.jdbc.JdbcManager;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by ♠K on 2017-8-2.
 * QQ:272488352
 */
public class DimensionConvert$Test {
    @Test
    public void getfindDimensionId() throws Exception {
        ;

        LocationDimension dimension = new LocationDimension("中国", "河北", "石家庄");
        System.out.println(DimensionConvert.getfindDimensionId(dimension, JdbcManager.getConnection()));
        ;
    }

}