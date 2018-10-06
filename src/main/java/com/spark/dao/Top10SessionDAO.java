package com.spark.dao;

import com.spark.pojo.Top10Session;
import com.spark.util.JDBCUtils;


public class Top10SessionDAO {
    public void insert(Top10Session top10Session) {
        String sql = "insert into top10_session values(?,?,?,?)";
        Object[] params = new Object[]{top10Session.getTaskId(),
                        top10Session.getCategoryId(),
                        top10Session.getSessionId(),
                        top10Session.getClickCount()};

        // 插入到mysql中
        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
        jdbcUtils.executeUpdate(sql, params);
    }
}
