package com.spark.accumulator;

import com.spark.constant.Constants;
import com.spark.util.MyStringUtils;
import org.apache.spark.AccumulatorParam;

/**
 *  实现AccumulatorParam的方式, 在spark 2.0中已过时
 *
 *  自定义 accumulator 中不仅可以使用 String 类型的泛型, 还可以使用自定义的数据类型, 比如自定义javaBean(必须可序列化)
 *  然后根据自定义的数据类型, 去实现一些复杂的分布式的计算逻辑
 *  各个task, 任务分布式运行的时候, 可以根据具体的需求, 给 accumulator 传入不同的参数
 *  同时, 在进行add方法时, 还可以更新 mysql, redis, hbase等操作, 可以实现复杂的逻辑
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {
    /**
     * 自定义 accumulator, 用于统计session中各个时间段的访问占比(spark 1.6.0)
     * 需求:
     * 1. 需要统计筛选后用户访问 session 中的session的访问时长, 访问步长
     * 2. 对于每个session的访问时长, 和访问步长, 统计其中访问步长在 0s-3s, 4s-6s...区间内占总session的比例
     * 3. 由于统计任务是分布式的, 需要解决并发和锁的问题, 如何不使用accumulator就只能借助其他方式, 维护中间状态信息
     * 4. 对于每一个区间如果使用一个accumulator, 最终会导致 accumulator 过多
     * 5. 使用自定义 accumulator 解决以上问题
     */

    /**
     *  zero方法: 主要用于数据的初始化
     *      各个区间范围内的session数量的统计, 采用的格式是 session1=0\session2=0\...
     */
    @Override
    public String zero(String initialValue) {
        return Constants.SESSION_COUNT + "=0|"
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


    /**
     * AddInPlace方法和AddAccumulator方法类似
     *      每次根据传入的结果更新统计结果, 然后返回最新的统计结果
     * @param r1 需要统计结果, 也就是在zero中初始化的字符串
     * @param r2 当在外界判断出session属于某一个区间, 比如Constants.TIME_PERIOD_1s_3s时, 此时r2传入的参数就是 Constants.TIME_PERIOD_1s_3s
     * @return  返回更新后的 统计字符串
     */
    @Override
    public String addInPlace(String r1, String r2) {
        return add(r1, r2);
    }

    @Override
    public String addAccumulator(String t1, String t2) {
        return add(t1, t2);
    }

    /**
     * 自定义累加方法
     * @param v1 统计连接串
     * @param v2 范围区间
     * @return 更新后的连接串
     */
    private String add(String v1, String v2) {
        //1 校验, 如果v1 为空, 直接返回 v2 (可能v1 还没有初始化, 此时v2为传入的连接串)
        if(MyStringUtils.isEmpty(v1)) return v2;

        //2 从v1 中提取 v2 对应的值, 并累加 1
        String oldValue = MyStringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if (oldValue != null) {
            String newValue = "" + (Integer.parseInt(oldValue) + 1);
            return MyStringUtils.setFieldInConcatString(v1, "\\|", v2, newValue);
        }
        return v1;
    }
}
