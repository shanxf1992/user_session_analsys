package com.spark.accumulator;

import com.spark.constant.Constants;
import com.spark.util.MyStringUtils;
import org.apache.spark.util.AccumulatorV2;

import java.io.Serializable;

/**
 * 自定义 accumulator, 用于统计session中各个时间段的访问占比(spark 2.0.0)
 * 需求:
 * 1. 需要统计筛选后用户访问 session 中的session的访问时长, 访问步长
 * 2. 对于每个session的访问时长, 和访问步长, 统计其中访问步长在 0s-3s, 4s-6s...区间内占总session的比例
 * 3. 由于统计任务是分布式的, 需要解决并发和锁的问题, 如何不使用accumulator就只能借助其他方式, 维护中间状态信息
 * 4. 对于每一个区间如果使用一个accumulator, 最终会导致 accumulator 过多
 * 5. 使用自定义 accumulator 解决以上问题
 */
public class SessionAggrStatAccumulatorV2 extends AccumulatorV2<String, String> implements Serializable {

    // 初始化值
    String value = Constants.SESSION_COUNT + "=0|"
            + Constants.TIME_PERIOD_1s_3s + "=0|"
            + Constants.TIME_PERIOD_4s_6s + "=0|"
            + Constants.TIME_PERIOD_7s_9s + "=0|"
            + Constants.TIME_PERIOD_10s_30s + "=0|"
            + Constants.TIME_PERIOD_30s_60s + "=0|"
            + Constants.TIME_PERIOD_1m_3m + "=0|"
            + Constants.TIME_PERIOD_3m_10m + "=0|"
            + Constants.TIME_PERIOD_10m_30m + "=0|"
            + Constants.TIME_PERIOD_30m + "=0|"
            + Constants.STEP_PERIOD_1_3 + "=0|"
            + Constants.STEP_PERIOD_4_6 + "=0|"
            + Constants.STEP_PERIOD_7_9 + "=0|"
            + Constants.STEP_PERIOD_10_30 + "=0|"
            + Constants.STEP_PERIOD_30_60 + "=0|"
            + Constants.STEP_PERIOD_60 + "=0";

    //Returns if this accumulator is zero value or not.
    // e.g. for a counter accumulator, 0 is zero value; for a list accumulator, Nil is zero value.
    @Override
    public boolean isZero() {
        return  (Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0").equals(this.value);
    }

    /**
     * 拷贝一个新的AccumulatorV2
     */
    @Override
    public AccumulatorV2<String, String> copy() {
        SessionAggrStatAccumulatorV2 newAccumulator = new SessionAggrStatAccumulatorV2();
        newAccumulator.value = this.value;
        return newAccumulator;
    }

    //重置AccumulatorV2中的数据
    @Override
    public void reset() {
        this.value = Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }
    //操作数据累加方法实现
    @Override
    public void add(String val) {

        //2 从v1 中提取 v2 对应的值, 并累加 1
        String oldValue = MyStringUtils.getFieldFromConcatString(value, "\\|", val);
        if (oldValue != null) {
            String newValue = "" + (Long.parseLong(oldValue) + 1);
            this.value = MyStringUtils.setFieldInConcatString(value, "\\|", val, newValue);
        }
    }
    //merge用于将另一个相同类型的累加器合并到该累加器中
    //session_count=0|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0|30_60=0|60=0
    @Override
    public void merge(AccumulatorV2<String, String> otherAccumulator) {
        SessionAggrStatAccumulatorV2 other = (SessionAggrStatAccumulatorV2) otherAccumulator;

        String[] fields = value.split("\\|"); //[session_count=3, 1s_3s=1, 4s_6s=9, 7s_9s=3, 10s_30s=3, 30s_60s=4]
        String[] otherFields = other.value().split("\\|"); //[session_count=9, 1s_3s=6, 4s_6s=5, 7s_9s=4, 10s_30s=11, 30s_60s=12]

        //合并两个累加器的对应的值
        for (int i = 0; i < fields.length; i++) {
            Long newValue = Long.parseLong(fields[i].split("=")[1]) + Long.parseLong(otherFields[i].split("=")[1]);
            this.value = MyStringUtils.setFieldInConcatString(this.value, "\\|", fields[i].split("=")[0], newValue.toString());
        }
    }

    //AccumulatorV2对外访问的数据结果
    @Override
    public String value() {
        return this.value;
    }
}
