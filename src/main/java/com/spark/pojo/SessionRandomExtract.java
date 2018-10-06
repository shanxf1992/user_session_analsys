package com.spark.pojo;

import java.io.Serializable;

/**
 * 随机抽取的 session
 */
public class SessionRandomExtract implements Serializable{
    private Long taskId;

    private String sessionId;

    private String startTime;

    private String searchKeywords;

    private String clickCategoryIds;

    public void setTaskId(Long taskId) {
        this.taskId = taskId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public void setSearchKeywords(String searchKeywords) {
        this.searchKeywords = searchKeywords;
    }

    public void setClickCategoryIds(String clickCategoryIds) {
        this.clickCategoryIds = clickCategoryIds;
    }

    public Long getTaskId() {
        return taskId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getSearchKeywords() {
        return searchKeywords;
    }

    public String getClickCategoryIds() {
        return clickCategoryIds;
    }
}