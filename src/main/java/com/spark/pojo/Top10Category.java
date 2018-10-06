package com.spark.pojo;

import java.io.Serializable;

public class Top10Category implements Serializable{
    private Long taskId;

    private Long categoryId;

    private Long clickCount;

    private Long orderCount;

    private Long payCount;

    public Long getTaskId() {
        return taskId;
    }

    public void setTaskId(Long taskId) {
        this.taskId = taskId;
    }

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }

    public void setOrderCount(Long orderCount) {
        this.orderCount = orderCount;
    }

    public void setPayCount(Long payCount) {
        this.payCount = payCount;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public Long getClickCount() {
        return clickCount;
    }

    public Long getOrderCount() {
        return orderCount;
    }

    public Long getPayCount() {
        return payCount;
    }
}