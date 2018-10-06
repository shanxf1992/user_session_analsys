package com.spark.pojo;

import java.io.Serializable;

public class Top10Session implements Serializable{
    private Long taskId;

    private Long categoryId;

    private String sessionId;

    private Long clickCount;

    public Long getTaskId() {
        return taskId;
    }

    public void setTaskId(Long taskId) {
        this.taskId = taskId;
    }

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public Long getClickCount() {
        return clickCount;
    }
}