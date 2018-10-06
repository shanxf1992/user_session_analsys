package com.spark.dao;

import com.spark.pojo.SessionRandomExtract;
import com.spark.util.JDBCUtils;

public class SessionRandomExtractDAO {

    /**
     * 插入session采样后的结果
     * @param sessionRandomExtract
     */
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values(?,?,?,?,?)";

        Object[] params = new Object[]{sessionRandomExtract.getTaskId(),
                sessionRandomExtract.getSessionId(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()};

        // 插入到mysql中
        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
        jdbcUtils.executeUpdate(sql, params);
    }
}
