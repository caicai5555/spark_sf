package com.daoke360.task.spark_rdd.etl.utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by ♠K on 2017-7-14.
 * QQ:272488352
 */
public class UserAgentUtil$Test {
    @Test
    public void analysisUserAgent() throws Exception {
        System.out.println(UserAgentUtil.analysisUserAgent("Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 UBrowser/6.1.2716.5 Safari/537.36").toString());
    }

}