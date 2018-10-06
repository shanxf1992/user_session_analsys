package com.spark.dao;

import com.spark.pojo.SessionDetail;
import com.spark.util.JDBCUtils;

import java.util.ArrayList;
import java.util.List;

public class SessionDetailDAO {
    /**
     * 插入经过聚合随机才采样的session明细数据到数据库
     *
     * @param sessionDetail
     */
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail "
                + "values(?,?,?,?,?,?,?,?,?,?,?,?)";

        Object[] params = new Object[]{sessionDetail.getTaskId(),
        sessionDetail.getUserId(),
        sessionDetail.getSessionId(),
        sessionDetail.getPageId(),
        sessionDetail.getActionTime(),
        sessionDetail.getSearchKeyword(),
        sessionDetail.getClickCategoryId(),
        sessionDetail.getClickProductId(),
        sessionDetail.getOrderCategoryIds(),
        sessionDetail.getOrderProductIds(),
        sessionDetail.getPayCategoryIds(),
        sessionDetail.getPayProductIds()};

        // 插入到mysql中
        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
        jdbcUtils.executeUpdate(sql, params);
    }

    /**
     * 实现 sessinoDetial 的批量插入
     */
    public void insertBatch(List<SessionDetail> sessionDetailList) {
        String sql = "insert into session_detail "
                + "values(?,?,?,?,?,?,?,?,?,?,?,?)";

        List<Object[]> paramsList = new ArrayList<>();
        for (SessionDetail sessionDetail : sessionDetailList) {
            Object[] params = new Object[]{sessionDetail.getTaskId(),
                    sessionDetail.getUserId(),
                    sessionDetail.getSessionId(),
                    sessionDetail.getPageId(),
                    sessionDetail.getActionTime(),
                    sessionDetail.getSearchKeyword(),
                    sessionDetail.getClickCategoryId(),
                    sessionDetail.getClickProductId(),
                    sessionDetail.getOrderCategoryIds(),
                    sessionDetail.getOrderProductIds(),
                    sessionDetail.getPayCategoryIds(),
                    sessionDetail.getPayProductIds()};
            paramsList.add(params);
        }

        // 插入到mysql中
        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
        jdbcUtils.executeBatch(sql, paramsList);

    }
}
