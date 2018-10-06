package com.spark.spark;

import scala.math.Ordered;

import java.io.Serializable;


/**
 * 品类二次排序
 * 封装session中品类的点击, 下单, 支付次数, 用于二次排序
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {

    private Long clickCount;
    private Long orderCount;
    private Long payCount;

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }

    public void setOrderCount(Long orderCount) {
        this.orderCount = orderCount;
    }

    public void setPayCount(Long payCount) {
        this.payCount = payCount;
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


    /**
     * 大于判断方法 =
     * @param other
     * @return
     */
    @Override
    public boolean $greater(CategorySortKey other) {
        if(this.clickCount > other.getClickCount()) return true;
        if(this.clickCount == other.getClickCount() && this.orderCount > other.getOrderCount()) return true;
        if (this.clickCount == other.getClickCount() && this.orderCount == other.getOrderCount() && this.payCount > other.getPayCount())
            return true;

        return false;
    }
    /**
     * 大于等于 >=
     * @param other
     * @return
     */
    @Override
    public boolean $greater$eq(CategorySortKey other) {
        if($greater(other)) return true;
        if (this.clickCount == other.getClickCount() && this.orderCount == other.getOrderCount() && this.payCount == other.getPayCount())
            return true;

        return false;
    }

    /**
     * 小于 <
     * @param other
     * @return
     */
    @Override
    public boolean $less(CategorySortKey other) {
        if(this.clickCount < other.getClickCount()) return true;
        if(this.clickCount == other.getClickCount() && this.orderCount < other.getOrderCount()) return true;
        if (this.clickCount == other.getClickCount() && this.orderCount == other.getOrderCount() && this.payCount < other.getPayCount())
            return true;

        return false;
    }

    /**
     * 小于等于 <=
     * @param other
     * @return
     */
    @Override
    public boolean $less$eq(CategorySortKey other) {
        if($less(other)) return true;
        if (this.clickCount == other.getClickCount() && this.orderCount == other.getOrderCount() && this.payCount == other.getPayCount())
            return true;

        return false;
    }

    /**
     * 自定义排序规则
     * @param other
     * @return
     */
    @Override
    public int compare(CategorySortKey other) {
        if (this.clickCount - other.getClickCount() != 0) {
            return this.clickCount.intValue() - other.getClickCount().intValue();
        } else if (this.orderCount - other.getOrderCount() != 0) {
            return this.orderCount.intValue() - other.getOrderCount().intValue();
        } else {
            return this.payCount.intValue() - other.getPayCount().intValue();
        }
    }

    @Override
    public int compareTo(CategorySortKey other) {
        if (this.clickCount - other.getClickCount() != 0) {
            return this.clickCount.intValue() - other.getClickCount().intValue();
        } else if (this.orderCount - other.getOrderCount() != 0) {
            return this.orderCount.intValue() - other.getOrderCount().intValue();
        } else {
            return this.payCount.intValue() - other.getPayCount().intValue();
        }
    }





}
