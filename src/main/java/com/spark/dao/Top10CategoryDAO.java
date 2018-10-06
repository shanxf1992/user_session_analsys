package com.spark.dao;

import com.spark.pojo.Top10Category;
import com.spark.util.JDBCUtils;

public class Top10CategoryDAO {
    /**
     * 插入到数据库
     * @param top10Category
     */
    public void insert(Top10Category top10Category) {
        String sql = "insert into top10_category values(?,?,?,?,?)";
        Object[] params = new Object[]{top10Category.getTaskId(),
                        top10Category.getCategoryId(),
                        top10Category.getClickCount(),
                        top10Category.getOrderCount(),
                        top10Category.getPayCount()};

        // 插入到mysql中
        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
        jdbcUtils.executeUpdate(sql, params);
    }
}
