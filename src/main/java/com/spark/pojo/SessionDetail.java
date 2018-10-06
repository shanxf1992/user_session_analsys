package com.spark.pojo;

import java.io.Serializable;

/**
 * 经过聚合随机才采样的session明细数据
 */
public class SessionDetail implements Serializable{
    private Long taskId;

    private Long userId;

    private String sessionId;

    private Long pageId;

    private String actionTime;

    private String searchKeyword;

    private Long clickCategoryId;

    private Long clickProductId;

    private String orderCategoryIds;

    private String orderProductIds;

    private String payCategoryIds;

    private String payProductIds;

    public void setTaskId(Long taskId) {
        this.taskId = taskId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public void setPageId(Long pageId) {
        this.pageId = pageId;
    }

    public void setActionTime(String actionTime) {
        this.actionTime = actionTime;
    }

    public void setSearchKeyword(String searchKeyword) {
        this.searchKeyword = searchKeyword;
    }

    public void setClickCategoryId(Long clickCategoryId) {
        this.clickCategoryId = clickCategoryId;
    }

    public void setClickProductId(Long clickProductId) {
        this.clickProductId = clickProductId;
    }

    public void setOrderCategoryIds(String orderCategoryIds) {
        this.orderCategoryIds = orderCategoryIds;
    }

    public void setOrderProductIds(String orderProductIds) {
        this.orderProductIds = orderProductIds;
    }

    public void setPayCategoryIds(String payCategoryIds) {
        this.payCategoryIds = payCategoryIds;
    }

    public void setPayProductIds(String payProductIds) {
        this.payProductIds = payProductIds;
    }

    public Long getTaskId() {
        return taskId;
    }

    public Long getUserId() {
        return userId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public Long getPageId() {
        return pageId;
    }

    public String getActionTime() {
        return actionTime;
    }

    public String getSearchKeyword() {
        return searchKeyword;
    }

    public Long getClickCategoryId() {
        return clickCategoryId;
    }

    public Long getClickProductId() {
        return clickProductId;
    }

    public String getOrderCategoryIds() {
        return orderCategoryIds;
    }

    public String getOrderProductIds() {
        return orderProductIds;
    }

    public String getPayCategoryIds() {
        return payCategoryIds;
    }

    public String getPayProductIds() {
        return payProductIds;
    }
}