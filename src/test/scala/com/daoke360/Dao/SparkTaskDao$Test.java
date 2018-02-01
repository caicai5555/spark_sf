package com.daoke360.Dao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.daoke360.domain.SparkTask;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by ♠K on 2017-7-18.
 * QQ:272488352
 */
public class SparkTaskDao$Test {

    public static class Student {
        private int id;
        private String name;


        public Student() {
        }

        public Student(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    @Test
    public void findTaskById() throws Exception {
        //  System.out.println(SparkTaskDao.findTaskById(2L).toString());
        Student st = new Student(1, "xm");

        // System.out.println(JSON.toJSONString(st));

        /*Student st = JSON.parseObject("{\"id\":1,\"name\":\"xm\"}", Student.class);
        System.out.println(st.getId());
        System.out.println(st.getName());*/

        final JSONObject jsonObject = JSON.parseObject("{\"id\":1,\"name\":\"xm\"}");
        System.out.println(jsonObject.get("aaa"));

    }


}