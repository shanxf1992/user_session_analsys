package com.spark.spark;

import com.alibaba.fastjson.JSONObject;
import com.spark.accumulator.SessionAggrStatAccumulatorV2;
import com.spark.daofactory.DAOFactory;
import com.spark.conf.ConfigurationManager;
import com.spark.constant.Constants;
import com.spark.dao.*;
import com.spark.pojo.*;
import com.spark.mockdata.MockData;
import com.spark.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;
import java.util.*;

/**
 * 用户访问 session 分析 spark 作业
 *      接收用户创建的分析任务, 用户可能指定的条件如下:
 *          1. 时间范围
 *          2. 性别
 *          3. 年龄范围
 *          4. 城市 : 多选
 *          5. 职业 : 多选
 *          6. 搜索词 : 多个, 只要session中任何一个action搜索过指定的关键词, 就符合条件
 *          7. 点击品类 : 多个品类, 只要session中任何一个action点击指定的品类, 就符合条件
 *
 *          spark作业如何接受用户创建的任务:
 *          平台接受用户创建的任务后, 会将任务信息插入mysql的task表中, 任务的参数以json合适封装在task_param中,
 *          接着, 平台会执行 spark-submit shell 脚本, 并将taskid作为参数传递给 spark-submit shell 脚本
 *          spark-submit shell 脚本执行时, 可以接受参数运行
 */


/**
 * 代码重构
 * 聚合统计: 统计出访问时长, 访问步长, 各个时间范围的session占比
 *      思路一: 基于最初的 rangeData, 只进行了时间范围过滤的 user_visit_info表的信息
 *          1. 映射: rangeRDD<row> --> pariRdd<session_id, row>
 *          2. 将结果按照session_id聚合, 同时, 每一个group按照时间进行二次排序
 *          3. 统计每一个session_id 对应的session的访问时长, 访问步长
 *          4. 更新accumulator中的值, 计算每个时间段的session占比
 *          5. 将结果写入到mysql中
 *      存在问题 :
 *          1. 重新对 rangeRDD 进行映射, 代码重复, 多此一举
 *          2. 为了 session聚合的功能, 还要重新遍历一边 session, 之前的过滤操作, 已经遍历了一遍
 *      重构思路:
 *          1. 不要生成新的 RDD
 *          2. 不要重复遍历session 数据
 *          3. 可以在进行session聚合的同时, 统计session的访问时长和访问步长
 *          4. 在过滤的时候, 可以某个session通过筛选条件后, 然后将其访问时长, 访问步长累加到 accumulator 上
 *
 *      TODO:重构部分:
 *          1. 在进行session聚合的时候, 计算每个session的访问时长和访问步长
 *          2. 重构过滤的部分, 在过滤的部分中, 加入自定义的 accumulator , 实现不同时间范围内的session占比
 */
public class UserVisitSession_Restructure {
    public static void main(String[] args) {
        //1 构建 spark 上下文
        // SparkConf sparkConf = new SparkConf()
        //         .setAppName(Constants.SPARK_APP_NAME_SESSION)
        //         .setMaster("local");

        // JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //2 获取 SparkSession 对象

        /**
         * TODO: 使用Kryo序列化
         *  1. set"spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         *  2. 需要注册的自定义的类  CategorySortKey
         */
        SparkConf sparkConf = new SparkConf()
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.storage.memoryFraction", "0.4")
                .set("spark.shuffle.consolidateFiles","true")
                .registerKryoClasses(new Class[]{CategorySortKey.class, });

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .config(sparkConf)
                .appName(Constants.SPARK_APP_NAME_SESSION)
                .enableHiveSupport()
                .getOrCreate();


        //************************************************生成模拟数据***************************************************
        mockData(spark);

        /**
         * 进行 session 粒度的聚合
         *      如果根据用户创建任务时指定的参数, 进行行为过滤, 首先就得查询出来指定的任务
         *      首先要从 user_visit_action 表中查询出指定日期范围内的行为数据
         */

        //3. 创建数据库连接组件
        TaskDAO taskDAO = DAOFactory.getTaskDAO();

        //4. 查询出对应的需要执行的任务
        long taskId = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(taskId);
        // 解析用户任务的指定参数
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //5. 从 user_visit_action 表中查询出指定日期范围内的行为数据
        JavaRDD<Row> rangeRDD = getActionRDDByDateRange(spark, taskParam);

        /**
         * 为了便于后续使用, 将 rangeRDD 做一次映射, JavaRDD<Row> --> JavaRDD<session_id, Row>
         *      sessionDetilRDD
         *          所有的访问session明细RDD
         *          格式: <session_id, Row>
         */
        JavaPairRDD<String, Row> sessionDetilRDD = getSessionDetilRDD(rangeRDD);

        /**
         * 6. 将筛选的结果按照 session 粒度进行聚合(按照 session_id 进行 groupbykey)
         *      然后与 用户信息表 join , 得到的结果就是聚合后的且包含用户信息的 session 粒度的数据
         *
         *      aggregateRDD
         *          按照session粒度聚合
         */
        JavaPairRDD<String, String> aggregateRDD = aggregateBySession(rangeRDD, spark);

        //TODO: 创建 AccumulatorV2, 并注册, 统计不同时间的session占比
        // Accumulator<String> sessionTimeRangeStat = spark.sparkContext().accumulator("", new SessionAggrStatAccumulator());
        AccumulatorV2<String, String> sessionTimeRangeStat = new SessionAggrStatAccumulatorV2();
        spark.sparkContext().register(sessionTimeRangeStat);

        /**
         * 7. 接着针对 session 粒度的聚合数据, 按照使用者指定的筛选参数进行数据过滤
         *  filterSessionRDD :
         *      按照session粒度聚合
         *      符合筛选条件
         */
        JavaPairRDD<String, String> filterSessionRDD = filterSession(aggregateRDD, taskParam, sessionTimeRangeStat);
        System.out.println("value ; " + sessionTimeRangeStat.value());
        /**
         *
         *  获取按照session粒度聚合后, 并且符合条件的 session明细RDD
         *  aggrFilterSessionDetailRDD : 便于后续使用
         *      按照session粒度聚合
         *      符合筛选条件
         *      具有session访问的明细数据
         */
        JavaPairRDD<String, Row> aggrFilterSessionDetailRDD = getAggFilterSessionDetail(filterSessionRDD, sessionDetilRDD);


        //************************************************对符合条件的session按小时区间进行平均采样***************************************************
        /**
         * 8. 根据时间比例在24个小时中, 平均抽取100条数据session (按时间比例进行采样)
         *      1. 进行映射, RDD<row> -> RDD<time, row>
         *      2. 根据time 时间进行分组, 统计每一小时
         */
        sampleRandomSession(spark, taskId, filterSessionRDD, sessionDetilRDD);

        //************************************************统计session访问时长, 步长的占比*********************************
        /**
         * 9. 计算session中的各个范围的session占比
         *      计算访问时长的session占比
         *      计算访问步长的session占比
         *      将计算的结果写入到mysql当中
         *
         *      注意:
         *      使用 Accumulator 时, 从其中获取参数, 插入到数据库中, 一定要在有某一个 action 的操作以后才能进行
         *      如果没有 action 操作, 整个程序就不会运行
         *      必须在插入数据到MySQL之前, 执行一次 action 操作, 保证程序运行, 让 accumulator 中有值
         */
        // filterSessionRDD.count();
        calculateSessionStatAndPersist(sessionTimeRangeStat.value(), task.getTaskId());

        //************************************************统计热门商品top10***************************************************
        /**
         * 1. 从筛选后的session中, 获取所有访问的品类
         * 2. 计算每个品类的点击, 下单, 支付的次数
         * 3. 根据 品类的点击, 下单, 支付进行二次排序
         * 4. 获取排名前10 的热门品类
         * 5. 将结果插入到数据库中
         */
        List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10CategoryIds(taskId, aggrFilterSessionDetailRDD);

        //************************************************获取活跃session top10************************************************
        /**
         *  对于 top 10 热门品类, 获取每一个品类点击次数最多的 top10 session, 及其访问明细
         *  1. 获取符合条件的session的明细数据
         *  2. 对session进行聚合, 返回该session 对每一个品类的点击次数(flatMap <categoryId, <sessionn, count>)
         *  3. 按照品类id 分组去top10
         *
         *  TODO:代码重构
         *  1. 将符合条件的session 明细RDD(按照session聚合, 同时具有明细数据), 在getTop10CategoryIds()方法中生成提取为一个公共的RDD, 以便后续使用, 不用重复生成
         *  2. 将获取top10热门品类的结果生成一个 pairRDD, 以便后续使用
         */
        getTop10Session(spark, taskId, top10CategoryList, aggrFilterSessionDetailRDD);
        spark.close();
    }

    /**
     * 模拟数据(只有在本地模式下, 才会去注册临时表)
     * @param spark
     */
    private static void mockData(SparkSession spark) {
        //判断是否是本地模式
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, spark);
        }
    } //mockData()

    /**
     * 从 user_visit_action 表中筛选出指定日期范围内的记录
     * @return
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SparkSession spark, JSONObject param) {
        String startDate = ParamUtils.getParam(param, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(param, Constants.PARAM_END_DATE);

        String sql = "select * from user_visit_action " +
                "where date >= '" + startDate + "' and date <= '" + endDate + "'";

        Dataset<Row> dataset = spark.sql(sql);


        //return dataset.toJavaRDD().repartition(100);
        return dataset.toJavaRDD();
    }

    /**
     * 将按照时间过滤后的RDD, 进行一次映射, 以便后续使用
     * @param rangeRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionDetilRDD(JavaRDD<Row> rangeRDD) {
        return rangeRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(2), row);
            }
        });

        //TODO:使用 MapPartition的方式:
        // return rangeRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
        //     @Override
        //     public Iterator<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
        //         List<Tuple2<String, Row>> list = new ArrayList<>();
        //         while (iterator.hasNext()) {
        //             Row row = iterator.next();
        //             list.add(new Tuple2<>(row.getString(2), row));
        //         }
        //         return list.iterator();
        //     }
        // });
    }


    /**
     * 对用户访问行为数据进行 session 粒度的聚合
     * TODO:重构部分1: 在进行session聚合的时候, 计算每个session的访问时长和访问步长
     * @param actionRDD 行为数据 RDD
     * @return
     */
    private static JavaPairRDD<String, String> aggregateBySession(JavaRDD<Row> actionRDD, SparkSession spark) {
        // actionRDD 中的一个 Row , 就是一次用户行为记录, 点击, 搜索或者下单等
        // 1 将actionRDD<Row> 映射成 <session_id, Row> 的格式
        /**
         *  PairFunction 参数:
         *      第一个参数: 传入的参数
         *      第二个参数: 需要输出的key
         *      第三个参数: 需要输出的value
         */
        JavaPairRDD<String, Row> pairRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2), row);
            }
        });

        //2 按照 session_id 进行分组
        JavaPairRDD<String, Iterable<Row>> groupPairRDD = pairRDD.groupByKey();

        /**
         * 3 对每一个 session 分组聚合, 将 session 中所有的搜索词, 点击品类聚合起来
         *      输入格式: JavaPairRDD<String, Iterable<Row>>
         *      输出格式: JavaPairRDD<Long, String> : 聚合后的数据
         *
         *   TODO:重构部分:
         *      TODO:在进行聚合的同时计算出每一个session的访问时长, 访问步长
         */
        JavaPairRDD<Long, String> partAggInfoRDD = groupPairRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String sessionId = tuple._1;
                Iterable<Row> rows = tuple._2;

                // 遍历行为时, 将点搜索的关键词, 点击的品类聚合起来
                StringBuffer searchKeywords = new StringBuffer("");
                StringBuffer clickCategoryIds = new StringBuffer("");

                /**
                 * 遍历一个session 的所有行为, 提取每个行为的搜索词和点击品类
                 * 并不是每一个行为都有 搜索词和点击品类, 还有可能具有重复的搜索词
                 * 所以拼接时需要过滤掉 null 值 和 重复的
                 */
                Long userId = null;

                //TODO: 重构统计访问时长, 访问步长
                // session 的访问起始时间
                Date startTime = null;
                Date endTime = null;
                // session的访问步长
                int stepLen = 0;


                for (Row row : rows) {
                    if (userId == null) userId = row.getLong(1);

                    String keyword = row.getString(5);
                    Object categoryId = row.get(6);
                    // 过滤掉 空值 和 重复的值
                    if (MyStringUtils.isNotEmpty(keyword) && !searchKeywords.toString().contains(keyword)) {
                        searchKeywords.append(keyword).append(",");
                    }

                    if (categoryId != null && !searchKeywords.toString().contains(categoryId.toString())) {
                        clickCategoryIds.append(categoryId + ",");
                    }

                    Date actionTime = DateUtils.parseTime(row.getString(4));
                    //TODO: 计算访问时长, 访问步长
                    if (startTime == null) startTime = actionTime;
                    if (endTime == null) endTime = actionTime;
                    if(actionTime.before(startTime)) startTime = actionTime;
                    if (actionTime.after(endTime)) endTime = actionTime;
                    stepLen++;
                }

                //去除首位的 ","
                String searchKeywodsStr = MyStringUtils.trimComma(searchKeywords.toString());
                String clickCategoryIdsStr = MyStringUtils.trimComma(clickCategoryIds.toString());

                //TODO: 计算访问时长, 单位秒
                Long visitLen = (endTime.getTime() - startTime.getTime()) / 1000;
                /**
                 * 返回聚合结果 PariRDD<user_id, row>
                 *      注: 因为聚合后的结果后续还要和 用户信息表进行 join , 所以返回的 pairRDD 的 key 就不能是 session_id
                 *      而应该是 user_id
                 * 聚合数据拼接的格式: key=value|key=value
                 */
                String aggreateInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
                        Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) + "|" +
                        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywodsStr + "|" +
                        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIdsStr + "|" +
                        Constants.FIELD_VISIT_LENGTH + "=" + visitLen + "|" +
                        Constants.FIELD_STEP_LENGTH + "=" + stepLen;
                return new Tuple2<Long, String>(userId, aggreateInfo);
            }
        });

        /**
         * 4. 上一步的聚合结果中, 只有session中的点击和搜索行为
         * 需要和用户信息表 join, 将用户信息也 聚合到结果当中
         */
        //4.1 查询所有用户数据, 并将其映射为 RDD<user_id, row> 的格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = spark.sql(sql).toJavaRDD();

        //4.2 映射 JavaRDD<Row> => JavaRDD<user_id, row>
        JavaPairRDD<Long, Row> userInfoPairRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(row.getLong(0), row);
            }
        });

        //4.3 关联 行为聚合的结果和用户信息
        JavaPairRDD<Long, Tuple2<String, Row>> aggInfo = partAggInfoRDD.join(userInfoPairRDD);
        //JavaPairRDD<Long, Tuple2<String, Optional<Row>>> aggInfo = partAggInfoRDD.leftOuterJoin(userInfoPairRDD);

        //5 将 join 的结果进行聚合, 输出格式 : <session_id, row>
        JavaPairRDD<String, String> fullAggInfo = aggInfo.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                Tuple2<String, Row> tupleValue = tuple._2;
                String partAggInfo = tupleValue._1;
                Row userInfoRow = tupleValue._2;

                // 获取用户信息中的 年龄, 职业, 城市, 性别
                Integer age = userInfoRow.getInt(3);
                String job = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);

                //按照 key=value|key=value|...的格式进行拼接
                String fullAggInfo = partAggInfo + "|" +
                        Constants.FIELD_AGE + "=" + age + "|" +
                        Constants.FIELD_PROFESSIONAL + "=" + job + "|" +
                        Constants.FIELD_CITY + "=" + city + "|" +
                        Constants.FIELD_SEX + "=" + sex;

                String sessionId = MyStringUtils.getFieldFromConcatString(partAggInfo, "\\|", Constants.FIELD_SESSION_ID);

                return new Tuple2<>(sessionId, fullAggInfo);
            }
        });

        //TODO: reduce join 转 map join
        //TODO: join 方法可能会导致数据倾斜, 其中 partAggInfoRDD 的数据量可能还是比较大的. userInfoPairRDD的数量可能较小
        //TODO: 比较适合使用 将reduce join 转 map join的方式, 来解决数据倾斜的问题
        //TODO: 合并以上4.3, 5俩个操作
        // 第一步. 将 userInfoPairRDD 作为 广播变量, 广播出去
        // List<Tuple2<Long, Row>> userInfos = userInfoPairRDD.collect();
        // JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        // Broadcast<List<Tuple2<Long, Row>>> userInfosBroadcast = sc.broadcast(userInfos);

        // 第二步, partAggInfoRDD进行map操作, 同时与广播变量进行 map端的join
        // partAggInfoRDD.mapToPair(new PairFunction<Tuple2<Long, String>, String, String>() {
        //     @Override
        //     public Tuple2<String, String> call(Tuple2<Long, String> tuple2) throws Exception {
        //         // 获取广播变量的数据
        //         List<Tuple2<Long, Row>> userinfos = userInfosBroadcast.getValue();
        //         // 便于操作, 将其转化成一个map
        //         Map<Long, Row> userInfoMap = new HashMap<>();
        //         for (Tuple2<Long, Row> userinfo : userinfos) {
        //             userInfoMap.put(userinfo._1, userinfo._2);
        //         }
        //
        //         //获取当前用户的信息
        //         String partAggInfo = tuple2._2;
        //         Row userInfoRow = userInfoMap.get(tuple2._1);
        //
        //         // 获取用户信息中的 年龄, 职业, 城市, 性别
        //         Integer age = userInfoRow.getInt(3);
        //         String job = userInfoRow.getString(4);
        //         String city = userInfoRow.getString(5);
        //         String sex = userInfoRow.getString(6);
        //
        //         //按照 key=value|key=value|...的格式进行拼接
        //         String fullAggInfo = partAggInfo + "|" +
        //                 Constants.FIELD_AGE + "=" + age + "|" +
        //                 Constants.FIELD_PROFESSIONAL + "=" + job + "|" +
        //                 Constants.FIELD_CITY + "=" + city + "|" +
        //                 Constants.FIELD_SEX + "=" + sex;
        //
        //         String sessionId = MyStringUtils.getFieldFromConcatString(partAggInfo, "\\|", Constants.FIELD_SESSION_ID);
        //
        //         return new Tuple2<>(sessionId, fullAggInfo);
        //     }
        // });


        //TODO: sample采样数据倾斜的key, 单独join
        //TODO: 除了使用将reduce join 转 map join的方式, 来解决数据倾斜的问题的方式以外,还可以考虑使用 sample采样数据倾斜的key, 单独join的方式.
        // 对partAggInfoRDD进行采样 false: 不替换原来的rdd, 0.1 比例, 9 随机种子
        // JavaPairRDD<Long, String> sampleRDD = partAggInfoRDD.sample(false, 0.1, 9);
        // // 对sampleRDD 进行映射, <key , 1> 就是每个key的次数
        // JavaPairRDD<Long, Long> mappedSampleRDD = sampleRDD.mapToPair(new PairFunction<Tuple2<Long, String>, Long, Long>() {
        //     @Override
        //     public Tuple2<Long, Long> call(Tuple2<Long, String> tuple2) throws Exception {
        //         return new Tuple2<>(tuple2._1, 1L);
        //     }
        // });
        // // 统计每个key出现的次数
        // JavaPairRDD<Long, Long> computedRDD = mappedSampleRDD.reduceByKey(new Function2<Long, Long, Long>() {
        //     @Override
        //     public Long call(Long v1, Long v2) throws Exception {
        //         return v1 + v2;
        //     }
        // });
        // // 按照出现的次数排序, 获取出现次数最多的key
        // List<Tuple2<Long, Long>> list = computedRDD.mapToPair(new PairFunction<Tuple2<Long, Long>, Long, Long>() {
        //     @Override
        //     public Tuple2<Long, Long> call(Tuple2<Long, Long> tuple2) throws Exception {
        //         return new Tuple2<>(tuple2._2, tuple2._1);
        //     }
        // }).sortByKey(false).take(1);
        //
        // Long skewedkKey = list.get(0)._2;
        //
        // // 然后对原始的rdd作一个拆分
        // JavaPairRDD<Long, String> skewedRDD = partAggInfoRDD.filter(new Function<Tuple2<Long, String>, Boolean>() {
        //     @Override
        //     public Boolean call(Tuple2<Long, String> v1) throws Exception {
        //         return v1._1 == skewedkKey;
        //     }
        // });
        // JavaPairRDD<Long, String> noSkewedRDD = partAggInfoRDD.filter(new Function<Tuple2<Long, String>, Boolean>() {
        //     @Override
        //     public Boolean call(Tuple2<Long, String> v1) throws Exception {
        //         return v1._1 != skewedkKey;
        //     }
        // });
        //
        // //对userInfoPairRDD也进行一次过滤, 过滤出对应的 skewedkKey 的rdd
        // JavaPairRDD<String, Row> userInfoSkewedRDD = userInfoPairRDD.filter(new Function<Tuple2<Long, Row>, Boolean>() {
        //     @Override
        //     public Boolean call(Tuple2<Long, Row> v1) throws Exception {
        //         return v1._1 == skewedkKey;
        //     }
        // }).flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Row>, String, Row>() {
        //     @Override
        //     public Iterator<Tuple2<String, Row>> call(Tuple2<Long, Row> tuple2) throws Exception {
        //         List<Tuple2<String, Row>> list = new ArrayList<>();
        //         for (int i = 0; i < 100; i++) {
        //             list.add(new Tuple2<>(i + "_" + tuple2._1, tuple2._2));
        //         }
        //
        //         return list.iterator();
        //     }
        // });
        //
        // //然后分别和 userInfoPairRDD join(通过随机前缀确保其被打散), 最后合并结果
        // JavaPairRDD<Long, Tuple2<String, Row>> skewedJoinRDD = skewedRDD.mapToPair(new PairFunction<Tuple2<Long,String>, String, String>() {
        //     @Override
        //     public Tuple2<String, String> call(Tuple2<Long, String> tuple2) throws Exception {
        //         Random random = new Random();
        //         int prefix = random.nextInt(100);
        //         return new Tuple2<>(prefix + "_" + tuple2._1, tuple2._2);
        //     }
        // }).join(userInfoSkewedRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, Long, Tuple2<String, Row>>() {
        //     @Override
        //     public Tuple2<Long, Tuple2<String, Row>> call(Tuple2<String, Tuple2<String, Row>> tuple2) throws Exception {
        //         return new Tuple2<>(Long.parseLong(tuple2._1.split("_")[1]), tuple2._2);
        //     }
        // });
        // JavaPairRDD<Long, Tuple2<String, Row>> noSkewedJoinRDD = noSkewedRDD.join(userInfoPairRDD);
        //
        // JavaPairRDD<Long, Tuple2<String, Row>> aggInfo = skewedJoinRDD.union(noSkewedJoinRDD);

        return fullAggInfo;
    }

    /**
     * 过滤 session 数据
     * TODO:重构部分2:  重构过滤的部分, 在过滤的部分中, 加入自定义的 accumulator , 实现不同时间范围内的session占比
     * @param aggreateSessionRDD 按照session粒度聚合的数据
     * @param taskParam  指定过滤的参数
     * @param sessionTimeRangeStat 自定义累加器
     * @return 过滤后的数据
     */
    // 使用Accumulator 累加器
    //private static JavaPairRDD<String, String> filterSession(JavaPairRDD<String, String> aggreateSessionRDD, JSONObject taskParam,  Accumulator<String> sessionTimeRangeStat) {
    // 使用AccumulatorV2 累加器
    private static JavaPairRDD<String, String> filterSession(JavaPairRDD<String, String> aggreateSessionRDD, JSONObject taskParam,  AccumulatorV2<String, String> sessionTimeRangeStat) {
        // 把json格式的taskParam包装成string形式（k1=v1|k2=v2..），为了使用validutils
        // 为性能优化埋下伏笔
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");

        if (parameter.endsWith("\\|")) {
            parameter.substring(0, parameter.length() - 1);
        }

        //按照指定的条件进行过滤
        JavaPairRDD<String, String> filterRDD = aggreateSessionRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                //1. 从tuple 中获取聚合数据
                String sessionInfo = tuple._2;
                //2. 按照筛选条件进行过滤
                // 按照年龄分割, 可能是两个(在年龄范围内的数据), 也可能没有
                // 如果参数parameter 中没有指定 age的范围, 或者session中的age在指定的范围内, ValidUtils.between就返回true, 直接进行下一步操作
                // 只用session中的age 不再指定的参数范围内, ValidUtils.between就返回false, 才会进入到if中, 直接返回 false, 过滤掉该条数据
                if (!ValidUtils.between(sessionInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                    return false;
                }
                // 按照职业信息进行过滤()
                // 只有当session 的职业不在指定的职业信息中,才会进入if判断, 直接过滤该条数据
                if (!ValidUtils.in(sessionInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
                    return false;
                }
                //按照城市信息进行过滤
                if (!ValidUtils.in(sessionInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
                    return false;
                }
                //按照性别进行过滤
                if (!ValidUtils.equal(sessionInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
                    return false;
                }
                //按照搜索词进行过滤
                // sessionInfo 中的搜索词有, 火锅,蛋糕,烧烤
                // 筛选中的指定的搜索词可能有 火锅,手机,电脑
                // ValidUtils.in方法判定: sessionInfo 中只要有一个词出现在 筛选的词中, 就返回true, 表示通过
                if (!ValidUtils.in(sessionInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                    return false;
                }
                //按照点击品类id 进行过滤
                if (!ValidUtils.in(sessionInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
                    return false;
                }

                //如果所有的条件都通过, 就进行下一步统计
                //TODO:重构, 重构过滤的部分, 在过滤的部分中, 加入自定义的 accumulator , 实现不同时间范围内的session占比
                // 1. 统计符合条件的session的数量
                sessionTimeRangeStat.add(Constants.SESSION_COUNT);
                // 2. 统计session中访问时长的所在的范围, 以及访问步长所在的范围
                String visitLen = MyStringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_VISIT_LENGTH);
                String visitStep = MyStringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_STEP_LENGTH);
                calculateVisitLen(visitLen);
                caculateVisitStep(visitStep);

                return true;
            }

            /**
             * 计算具体的访问时长的范围
             * @param visitLen
             */
            private void calculateVisitLen(String visitLen) {
                if (visitLen == null) return;
                Long visitTime = Long.parseLong(visitLen);
                if (visitTime >= 1 && visitTime <= 3) sessionTimeRangeStat.add(Constants.TIME_PERIOD_1s_3s);
                else if (visitTime >= 4 && visitTime <= 6) sessionTimeRangeStat.add(Constants.TIME_PERIOD_4s_6s);
                else if (visitTime >= 7 && visitTime <= 9) sessionTimeRangeStat.add(Constants.TIME_PERIOD_7s_9s);
                else if (visitTime >= 10 && visitTime <= 30) sessionTimeRangeStat.add(Constants.TIME_PERIOD_10s_30s);
                else if (visitTime > 30 && visitTime <= 60) sessionTimeRangeStat.add(Constants.TIME_PERIOD_30s_60s);
                else if (visitTime > 60 && visitTime <= 180) sessionTimeRangeStat.add(Constants.TIME_PERIOD_1m_3m);
                else if (visitTime > 180 && visitTime <= 600) sessionTimeRangeStat.add(Constants.TIME_PERIOD_3m_10m);
                else if (visitTime > 600 && visitTime <= 1800) sessionTimeRangeStat.add(Constants.TIME_PERIOD_10m_30m);
                else if (visitTime > 1800) sessionTimeRangeStat.add(Constants.TIME_PERIOD_30m);
            }

            /**
             * 计算访问步长所在的范围
             * @param visitStep
             */
            private void caculateVisitStep(String visitStep) {
                if (visitStep == null) return;
                Long step = Long.parseLong(visitStep);
                if(step >= 1 && step <= 3) sessionTimeRangeStat.add(Constants.STEP_PERIOD_1_3);
                else if(step >= 4 && step <= 6) sessionTimeRangeStat.add(Constants.STEP_PERIOD_4_6);
                else if(step >= 7 && step <= 9) sessionTimeRangeStat.add(Constants.STEP_PERIOD_7_9);
                else if(step >= 10 && step <= 30) sessionTimeRangeStat.add(Constants.STEP_PERIOD_10_30);
                else if(step > 30 && step <= 60) sessionTimeRangeStat.add(Constants.STEP_PERIOD_30_60);
                else if(step > 60) sessionTimeRangeStat.add(Constants.STEP_PERIOD_60);
            }

        });

        return filterRDD;
    }

    /**
     * 获取符合条件, 经过聚合的具有明细数据的session
     *
     * @param filterSessionRDD 经过聚合筛选的RDD
     * @param sessionDetilRDD  具有session访问明细的RDD
     * @return
     */
    private static JavaPairRDD<String, Row> getAggFilterSessionDetail(JavaPairRDD<String, String> filterSessionRDD, JavaPairRDD<String, Row> sessionDetilRDD) {
        return filterSessionRDD.join(sessionDetilRDD).mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple2) throws Exception {
                return new Tuple2<String, Row>(tuple2._1, tuple2._2._2);
            }
        });
    }

    /**
     * 按照每天每小时对 RDD 进行随机抽样
     * @param sessinoRDD 需要抽样的 sessin 的索引
     */
    private static void sampleRandomSession(SparkSession spark, long taskId, JavaPairRDD<String, String> sessinoRDD, JavaPairRDD<String, Row> sessionDetilRDD) {
         // 1. 进行映射, RDD<row> -> RDD<yyyy-MM-dd_HH, sessionRDD>
        JavaPairRDD<String, String> dateAggrSessionRDD = sessinoRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                String sessionInfo = tuple._2;
                //获取开始时间
                String time = MyStringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_START_TIME);
                String dateHour = DateUtils.getDateHour(time); // 解析出时间  格式 2018-03-23_09

                //在统计每个小时的session前, 需要将聚合的数据的信息写入到 session_random_extract 表中
                // 所以返回的结果为, <时间, 聚合数据>
                return new Tuple2<String, String>(dateHour, sessionInfo);
            }
        });

        // 2. 根据time 时间进行分组统计, 统计每一小时对应的session的个数
        Map<String, Long> countMap = dateAggrSessionRDD.countByKey();

        // 3. 按照时间比例随机抽取session的算法, 得到每天每个小时内抽取的session的索引
        // 将<yyyy-MM-dd_HH, count>格式的 countMap 转换成<yyyy-MM--dd, <HH, count>> 的格式
        HashMap<String, HashMap<String, Long>> dayHourCountMap = new HashMap<>();

        for (Map.Entry<String, Long> entry : countMap.entrySet()) {
            String time = entry.getKey();
            Long count = entry.getValue();

            String day = time.split("_")[0];
            String hour = time.split("_")[1];

            //将转换后的map添加到dayHourCountMap中
            HashMap<String, Long> hourCountMap = dayHourCountMap.get(day);

            if (hourCountMap == null) {
                hourCountMap = new HashMap<>();
                dayHourCountMap.put(day, hourCountMap);
            }
            hourCountMap.put(hour, count);
        }

        //4 计算每个小时内需要抽取的 session的个数, 假设需要抽取100个
        // 每一天需要抽取的数量
        long extractNumberPerDay = 100 / dayHourCountMap.size();

        //需要保存的结构 <day, <hour, [2,3,40...]>>
        //TODO: dayHourExtractMap 变量在后续的算子中使用, 每个task都会拷贝一份副本. 可以将其做成广播变量
        Map<String, Map<String, List<Integer>>> dayHourExtractMap = new HashMap<>();

        Random random = new Random();
        // 计算每天每个小时需要采样的数据量, 然后生成随机的索引值
        // entry : <yyyy-MM--dd, <HH, count>>
        for (Map.Entry<String, HashMap<String, Long>> entry : dayHourCountMap.entrySet()) {
            // 1. 获取每天需要采样的数量
            String day = entry.getKey();
            Map<String, Long> hourCountMap = entry.getValue();
            // 2. 计算出这一天的session的总数
            long daySessionCount = 0L;
            for (Long hourCount : hourCountMap.values()) {
                daySessionCount += hourCount;
            }

            // 存放当天每一个小时采样随机数的map
            Map<String, List<Integer>> hourExtractMap = dayHourExtractMap.get(day);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<>();
                dayHourExtractMap.put(day, hourExtractMap);
            }

            // 3. 遍历每个小时, 计算每一个小时需要采样的数据
            // hourEntry : <HH, count>
            for (Map.Entry<String, Long> hourEntry : hourCountMap.entrySet()) {
                String hour = hourEntry.getKey();
                long hourCount = hourEntry.getValue();

                //4. 计算每个小时的session的数量, 占当天session数量的比例, 乘以当天需要抽取的数量, 得到这一小时内需要采样的数量
                // 当前一小时内, 需要采样的条数
                long hourExtractNumber = (long) ((double) hourCount / (double) daySessionCount * extractNumberPerDay);
                if(hourExtractNumber > hourCount) hourExtractNumber = hourCount;
                //5. 根据采样的数量, 生成随机数, 将其添加到存放随机数的 list 中
                // 获取存放当前小时的采样随机数的 List
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<>();
                    hourExtractMap.put(hour, extractIndexList);
                }
                //生成随机数(索引)
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) hourCount);
                    while (extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) hourCount);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }

        /**
         *TODO: 将 dayHourExtractMap 变量变为广播变量
         */
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Broadcast<Map<String, Map<String, List<Integer>>>> dayHourExtractMapBroadcast = sc.broadcast(dayHourExtractMap);

        //4 . 遍历每天每天小时对应的 sessionRDD , 根据生成的随机索引, 进行抽取
        // timeGroupSessionRDD: <yyyy-MM-dd_HH, [sessionAggInfo1, sessionAggInfo2...]>
        JavaPairRDD<String, Iterable<String>> timeGroupSessionRDD = dateAggrSessionRDD.groupByKey();

        // 5. 使用 flatMap 算子, 遍历所有的sessionAggRDD, 会遍历每天每小时的 session, 如果发现某个session在指定的随机抽取的索引上,
        // 就抽取该session 插入到MySQL 中, 抽取出来的 RDD, 会返回为一个新的 RDD, 在和明细数据进行 join, 写入session表

        JavaPairRDD<String, String> extractSessionRDD = timeGroupSessionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {

            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                List<Tuple2<String, String>> extractSessionIds = new ArrayList<>();

                // 获取当前时间, 查询这一小时内采样的索引值
                String[] fields = tuple._1.split("_");
                String day = fields[0];
                String hour = fields[1];

                //TODO: 使用广播变量的时候直接调用其value()方法
                //Map<String, Map<String, List<Integer>>> dayHourExtractMap = dayHourExtractMapBroadcast.getValue();
                //获取当前天, 当前小时采样的索引值
                List<Integer> extractIndexList = dayHourExtractMap.get(day).get(hour);

                // 获取DAO, 向数据库中插入采样的数据
                SessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();

                // 获取每个session信息, 判断是否时要采样的数据
                Iterable<String> iterable = tuple._2;
                int index = 0;
                for (String aggSessionInfo : iterable) {
                    // 如果满足, 说明是采样的数据, 需要提取出sessionid, 然后写入到数据库
                    if (extractIndexList.contains(index)) {
                        String session_id = MyStringUtils.getFieldFromConcatString(aggSessionInfo, "\\|", Constants.FIELD_SESSION_ID);
                        // 封装实体类
                        SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                        sessionRandomExtract.setTaskId(taskId);
                        sessionRandomExtract.setSessionId(session_id);
                        sessionRandomExtract.setStartTime(MyStringUtils.getFieldFromConcatString(aggSessionInfo, "\\|", Constants.FIELD_START_TIME));
                        sessionRandomExtract.setSearchKeywords(MyStringUtils.getFieldFromConcatString(aggSessionInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                        sessionRandomExtract.setClickCategoryIds(MyStringUtils.getFieldFromConcatString(aggSessionInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
                        // 首先写入数据库
                        sessionRandomExtractDAO.insert(sessionRandomExtract);

                        //将session_id 添加到 list 中
                        extractSessionIds.add(new Tuple2<>(session_id, session_id));
                    }
                    index++;
                }
                return extractSessionIds.iterator();
            }
        });


        // 6. 通过和Session明细RDD关联, 获取抽取出来的明细数据
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetilRDD = extractSessionRDD.join(sessionDetilRDD);

        // 7. 将抽取出来的明细数据插入数据库
        extractSessionDetilRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                // 获取
                Row row = tuple._2._2;
                // 封装SessionDetail数据
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskId(taskId);
                sessionDetail.setUserId(row.isNullAt(1) ? null : row.getLong(1));
                sessionDetail.setSessionId(row.isNullAt(2) ? null : row.getString(2));
                sessionDetail.setPageId(row.isNullAt(3) ? null : row.getLong(3));
                sessionDetail.setActionTime(row.isNullAt(4) ? null : row.getString(4));
                sessionDetail.setSearchKeyword(row.isNullAt(5) ? null : row.getString(5));
                sessionDetail.setClickCategoryId( row.isNullAt(6) ? null : row.getLong(6));
                sessionDetail.setClickProductId(row.isNullAt(7) ? null : row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.isNullAt(8) ? null : row.getString(8));
                sessionDetail.setOrderProductIds(row.isNullAt(9) ? null : row.getString(9));
                sessionDetail.setPayCategoryIds(row.isNullAt(10) ? null : row.getString(10));
                sessionDetail.setPayProductIds(row.isNullAt(11) ? null : row.getString(11));

                //插入到数据库
                SessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });
    }

    /**
     * 计算session的统计范围占比, 并将计算结果导入到mysql
     * @param value 自定义累加器 accumulator 的值
     */
    private static void calculateSessionStatAndPersist(String value, Long taskId) {
        // 从Accumulator统计串中获对应的统计结果
        long session_count = Long.parseLong(
                MyStringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));

        //访问时长
        long visit_length_1s_3s = Long.parseLong(
                MyStringUtils.getFieldFromConcatString( value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.parseLong(
                MyStringUtils.getFieldFromConcatString( value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.parseLong(
                MyStringUtils.getFieldFromConcatString( value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.parseLong(
                MyStringUtils.getFieldFromConcatString( value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.parseLong(
                MyStringUtils.getFieldFromConcatString( value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.parseLong(
                MyStringUtils.getFieldFromConcatString( value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.parseLong(
                MyStringUtils.getFieldFromConcatString( value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.parseLong(
                MyStringUtils.getFieldFromConcatString( value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.parseLong(
                MyStringUtils.getFieldFromConcatString( value, "\\|", Constants.TIME_PERIOD_30m));

        //访问步长
        long step_length_1_3 = Long.parseLong(
                MyStringUtils.getFieldFromConcatString( value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.parseLong(
                MyStringUtils.getFieldFromConcatString( value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.parseLong(
                MyStringUtils.getFieldFromConcatString(  value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.parseLong(
                MyStringUtils.getFieldFromConcatString( value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.parseLong(
                MyStringUtils.getFieldFromConcatString( value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.parseLong(
                MyStringUtils.getFieldFromConcatString( value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double) step_length_60 / (double) session_count, 2);

        //将统计结果封装到SessionAggrStat pojo中
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskId);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 插入结果到数据库
        SessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }

    /**
     * 获取top10 热门品类
     * @param sessionDetail 经过筛选过后的聚合数据
     */
    private static List<Tuple2<CategorySortKey, String>>  getTop10CategoryIds(Long taskId, JavaPairRDD<String, Row> sessionDetail ) {
        //1. 获取session中访问过的所有的品类 TODO: 重构后抽取出来
        // 首先, 获取符合条件的session的详细信息
        // JavaPairRDD<String, Row> sessionDetail =
        //         aggrSessionRDD.join(sessionDetilRDD)
        //         .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
        //             @Override
        //             public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
        //                 return new Tuple2<>(tuple._1, tuple._2._2);
        //             }
        // });

        // 然后获取 session 访问过的所有的品类 id
        // 访问过包括 点击, 下单, 支付等行为
        // 然后对结果去重 distinct
        JavaPairRDD<Long, Long> categoryIdRDD = sessionDetail.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                //定义一个set集合, 存放当前的访问过的品类, 可以避免重复
                Set<Tuple2<Long, Long>> categoryId = new HashSet<>();
                Row row = tuple._2;
                // 索引6 为点击的品类信息
                if (!row.isNullAt(6)) categoryId.add(new Tuple2<>(row.getLong(6), row.getLong(6)));
                // 索引8 为下单的品类行为
                if (!row.isNullAt(8)) {
                    String[] ids = row.getString(8).split(",");
                    for (String id : ids) {
                        categoryId.add(new Tuple2<>(Long.parseLong(id), Long.parseLong(id)));
                    }
                }
                //索引10 为支付的品类的信息
                if (!row.isNullAt(10)) {
                    String[] ids = row.getString(10).split(",");
                    for (String id : ids) {
                        categoryId.add(new Tuple2<>(Long.parseLong(id), Long.parseLong(id)));
                    }
                }

                return categoryId.iterator();
            }
        }).distinct();

        //2. 计算每个品类的点击,下单,支付的次数
        //首先, 分别计算 点击, 下单, 支付的次数
        // 对品类的点击RDD 进行映射
        JavaPairRDD<Long, Long> clickCategoryRDD = sessionDetail.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                // 如果点击行为为空, 就返回true
                return !tuple._2.isNullAt(6);
            }
        }).mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
                return new Tuple2<>(tuple._2.getLong(6), 1L);
            }
        });

        //2.1 计算每个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryCountRDD = clickCategoryRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long value1, Long value2) throws Exception {
                return value1 + value2;
            }
        });

        //TODO: 重构2.1 计算每个品类的点击次数 的代码
        //TODO: 针对于 reducebykey的操作可能导致的数据倾斜的问题, 进行优化, 使用随机key, 进行双重聚合.
        //第一步. 给每个key, 打上一个随机数
        // JavaPairRDD<String, Long> mappedClickCategoryCountRDD = clickCategoryCountRDD.mapToPair(new PairFunction<Tuple2<Long, Long>, String, Long>() {
        //     @Override
        //     public Tuple2<String, Long> call(Tuple2<Long, Long> tuple2) throws Exception {
        //         // 创建一个随机对象
        //         Random random = new Random();
        //         // 随机生成一个10 以内的随机前缀
        //         int prefix = random.nextInt(10);
        //         // 给每个key, 打上一个随机数
        //         return new Tuple2<>(prefix + "_" + tuple2._1, tuple2._2);
        //     }
        // });
        //
        // //第二步.执行第一轮局部聚合, 此时key时完全打散的
        // JavaPairRDD<String, Long> firstAggrRDD = mappedClickCategoryCountRDD.reduceByKey(new Function2<Long, Long, Long>() {
        //     @Override
        //     public Long call(Long v1, Long v2) throws Exception {
        //         return v1 + v2;
        //     }
        // });
        // //第三步. 去除掉每个key的前缀
        // JavaPairRDD<Long, Long> restoredRDD = firstAggrRDD.mapToPair(new PairFunction<Tuple2<String, Long>, Long, Long>() {
        //     @Override
        //     public Tuple2<Long, Long> call(Tuple2<String, Long> tuple2) throws Exception {
        //         String key = tuple2._1.split("_")[1];
        //         return new Tuple2<>(Long.parseLong(key), tuple2._2);
        //     }
        // });
        // //第四步. 做第二轮全局的聚合, 也就时最初的要优化的 reducebykey的代码
        // JavaPairRDD<Long, Long> globalAggrRDD = restoredRDD.reduceByKey(new Function2<Long, Long, Long>() {
        //     @Override
        //     public Long call(Long value1, Long value2) throws Exception {
        //         return value1 + value2;
        //     }
        // });
        //TODO: 经过两轮的聚合, 通过这种方式解决数据倾斜的问题

        //2.2 计算每个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryCountRDD = sessionDetail.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                // 如果点击行为为空, 就返回true
                return !tuple._2.isNullAt(8);
            }
        }).flatMapToPair(new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                String[] orderCategoryIds = tuple._2.getString(8).split(",");
                List<Tuple2<Long, Long>> list = new ArrayList<>();
                for (String id : orderCategoryIds) {
                    list.add(new Tuple2<>(Long.parseLong(id), 1L));
                }
                return list.iterator();
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long value1, Long value2) throws Exception {
                return value1 + value2;
            }
        });


        //2.3 计算每个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryCountRDD = sessionDetail.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                // 如果点击行为为空, 就返回true
                return !tuple._2.isNullAt(10);
            }
        }).flatMapToPair(new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                String[] payCategoryIds = tuple._2.getString(10).split(",");
                List<Tuple2<Long, Long>> list = new ArrayList<>();
                for (String id : payCategoryIds) {
                    list.add(new Tuple2<>(Long.parseLong(id), 1L));
                }
                return list.iterator();
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long value1, Long value2) throws Exception {
                return value1 + value2;
            }
        });

        //3. join 品类和点击,下单,支付的结果
        JavaPairRDD<Long, String> categoryDetailRDD =  categoryIdRDD.leftOuterJoin(clickCategoryCountRDD)
                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple2) throws Exception {
                        Long categoryId = tuple2._1;
                        Optional<Long> optional = tuple2._2._2;
                        long clickCount = optional.isPresent() ?  optional.get() : 0L;
                        String value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" +
                                Constants.FIELD_CLICK_COUNT + "=" + clickCount;
                        return new Tuple2<>(categoryId, value);
                    }
                })
                .leftOuterJoin(orderCategoryCountRDD)
                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple2) throws Exception {
                        long categoryId = tuple2._1;
                        Optional<Long> optional = tuple2._2._2;
                        long clickCount = optional.isPresent() ? optional.get() :  0L;
                        String value = tuple2._2._1 + "|" + Constants.FIELD_ORDER_COUNT + "=" + clickCount;
                        return new Tuple2<>(categoryId, value);
                    }
                })
                .leftOuterJoin(payCategoryCountRDD)
                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {

                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple2) throws Exception {
                        long categoryId = tuple2._1;
                        Optional<Long> optional = tuple2._2._2;
                        long clickCount = optional.isPresent() ? optional.get() : 0L;
                        String value = tuple2._2._1 + "|" + Constants.FIELD_PAY_COUNT + "=" + clickCount;
                        return new Tuple2<>(categoryId, value);
                    }
                });

        //4. 对品类信息进行二次排序, 按照点击, 下单, 支付的次数
        //4.1 首先将点击, 下单和支付的次数进行封装, 然后将其映射为key, 进行排序 <info> --> <key, info>
        JavaPairRDD<CategorySortKey, String> categoryInfoPairRDD = categoryDetailRDD.mapToPair(new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
            @Override
            public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple2) throws Exception {
                String clickCount = MyStringUtils.getFieldFromConcatString(tuple2._2, "\\|", Constants.FIELD_CLICK_COUNT);
                String orderCount = MyStringUtils.getFieldFromConcatString(tuple2._2, "\\|", Constants.FIELD_ORDER_COUNT);
                String payCount = MyStringUtils.getFieldFromConcatString(tuple2._2, "\\|", Constants.FIELD_PAY_COUNT);

                CategorySortKey categorySortKey = new CategorySortKey();
                categorySortKey.setClickCount(Long.parseLong(clickCount));
                categorySortKey.setOrderCount(Long.parseLong(orderCount));
                categorySortKey.setPayCount(Long.parseLong(payCount));

                return new Tuple2<>(categorySortKey, tuple2._2);
            }
        });

        //4.2 进行二次排序
        JavaPairRDD<CategorySortKey, String> sortCategoryRDD = categoryInfoPairRDD.sortByKey(false);

        //5. 去除 top10 的品类信息, 并写入数据库
        List<Tuple2<CategorySortKey, String>> top10 = sortCategoryRDD.take(10);
        for (Tuple2<CategorySortKey, String> tuple2 : top10) {
            String categoryId = MyStringUtils.getFieldFromConcatString(tuple2._2, "\\|", Constants.FIELD_CATEGORY_ID);

            Top10Category top10Category = new Top10Category();
            top10Category.setTaskId(taskId);
            top10Category.setCategoryId(Long.parseLong(categoryId));
            top10Category.setClickCount(tuple2._1.getClickCount());
            top10Category.setOrderCount(tuple2._1.getOrderCount());
            top10Category.setPayCount(tuple2._1.getPayCount());

            Top10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
            top10CategoryDAO.insert(top10Category);
        }

        return top10;
    }

    /**
     * 获取活跃session top10
     * @param taskId 任务Id
     * @param top10CategoryList top10热门商品信息
     * @param sessionDetail
     */
    private static void getTop10Session(SparkSession spark, Long taskId, List<Tuple2<CategorySortKey, String>> top10CategoryList, JavaPairRDD<String, Row> sessionDetail) {
        //1.生成热门品类对应的RDD
        List<Tuple2<Long, Long>> categoryIdList = new ArrayList<>();
        for (Tuple2<CategorySortKey, String> tuple2 : top10CategoryList) {
            String categoryId = MyStringUtils.getFieldFromConcatString(tuple2._2, "\\|", Constants.FIELD_CATEGORY_ID);
            categoryIdList.add(new Tuple2<>(Long.parseLong(categoryId), Long.parseLong(categoryId)));
        }

        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        JavaPairRDD<Long, Long> top10CategoryRDD = javaSparkContext.parallelizePairs(categoryIdList);

        //2. 计算top10 品类每个品类被不同session的点击次数
        // flatMapToPair() 返回格式: <categoryId, 'sessionId,count'>
        // join结果 : top10 热门品类被各个session点击的次数, 格式<categoryId, sessionId,count>
        JavaPairRDD<Long, String> top10CategorySessionCountRDD = sessionDetail.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Iterator<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
                Map<Long, Long> map = new HashMap<>();
                String SessionId = tuple2._1;
                // 获取当session 中, 对每个品类的点击次数
                for (Row row : tuple2._2) {
                    if (!row.isNullAt(6)) {
                        Long categoryId = row.getLong(6);
                        Long count = map.get(categoryId);
                        if (count == null) count = 0L;
                        count++;
                        map.put(categoryId, count);
                    }
                }

                List<Tuple2<Long, String>> list = new ArrayList<>();
                for (Map.Entry<Long, Long> entry : map.entrySet()) {
                    list.add(new Tuple2<>(entry.getKey(), SessionId + "," + entry.getValue()));
                }
                return list.iterator();
            }
        }).join(top10CategoryRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Long>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Long>> tuple2) throws Exception {
                return new Tuple2<>(tuple2._1, tuple2._2._1);
            }
        });

        //3. 分组取topN, 取每一个品类中点击次数多的前10 个session//
        // 最后将结果插入到MySQL中
        JavaPairRDD<String, String> top10Session = top10CategorySessionCountRDD.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {

            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple2) throws Exception {
                // tuple2 : <categoryId, [sessin1,count2 session2,count2...]>
                Long categoryId = tuple2._1;

                // 定义一个有序数据, 按照'sessionId,count'字符串的count大小
                String[] sessionCount10 = new String[10];

                Iterable<String> sessionCountList = tuple2._2;
                // 遍历每一个session,count 去count前10的session
                for (String sessionCount : sessionCountList) {
                    String count = sessionCount.split(",")[1];
                    for (int i = 0; i < 10; i++) {
                        //如果当前位置没有数据
                        if (sessionCount10[i] == null) {
                            sessionCount10[i] = sessionCount;
                            break;
                        } else {
                            // 否则判断该数据是否要插入到数据中, 需要进行比较
                            String _count = sessionCount10[i].split(",")[1];
                            if (Long.parseLong(count) > Long.parseLong(_count)) {
                                for (int j = 9; j > i; j--) {
                                    sessionCount10[j] = sessionCount10[j - 1];
                                }
                                sessionCount10[i] = sessionCount;
                                break;
                            }
                        }
                    }
                }

                List<Tuple2<String, String>> list = new ArrayList<>();

                Top10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
                // 将结果写入数据库中
                Top10Session top10Session = new Top10Session();
                for (String sessionCount : sessionCount10) {
                    if (sessionCount != null) {
                        String sessionId = sessionCount.split(",")[0];
                        String count = sessionCount.split(",")[1];
                        top10Session.setTaskId(taskId);
                        top10Session.setCategoryId(categoryId);
                        top10Session.setSessionId(sessionId);
                        top10Session.setClickCount(Long.parseLong(count));

                        top10SessionDAO.insert(top10Session);

                        list.add(new Tuple2<>(sessionId, sessionCount));
                    }
                }
                return list.iterator();
            }
        });

        //4. 获取top10活跃session的明细数据, 然后吸入到mysql中
        //TODO: 使用foreach插入数据库, 会多次获取数据连接, 同时执行多次插入语句
        // top10Session.join(sessionDetail).foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
        //     @Override
        //     public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
        //         // 获取
        //         Row row = tuple._2._2;
        //         // 封装SessionDetail数据
        //         SessionDetail sessionDetail = new SessionDetail();
        //         sessionDetail.setTaskId(taskId);
        //         sessionDetail.setUserId(row.isNullAt(1) ? null : row.getLong(1));
        //         sessionDetail.setSessionId(row.isNullAt(2) ? null : row.getString(2));
        //         sessionDetail.setPageId(row.isNullAt(3) ? null : row.getLong(3));
        //         sessionDetail.setActionTime(row.isNullAt(4) ? null : row.getString(4));
        //         sessionDetail.setSearchKeyword(row.isNullAt(5) ? null : row.getString(5));
        //         sessionDetail.setClickCategoryId( row.isNullAt(6) ? null : row.getLong(6));
        //         sessionDetail.setClickProductId(row.isNullAt(7) ? null : row.getLong(7));
        //         sessionDetail.setOrderCategoryIds(row.isNullAt(8) ? null : row.getString(8));
        //         sessionDetail.setOrderProductIds(row.isNullAt(9) ? null : row.getString(9));
        //         sessionDetail.setPayCategoryIds(row.isNullAt(10) ? null : row.getString(10));
        //         sessionDetail.setPayProductIds(row.isNullAt(11) ? null : row.getString(11));
        //
        //         //插入到数据库
        //         SessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
        //         sessionDetailDAO.insert(sessionDetail);
        //     }
        // });

        //TODO: 使用foreachPartition插入数据库, 一次处理一个partition的数据, 只发送一次sql, 获取一次数据库连接
        top10Session.join(sessionDetail).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>() {
            //一次传入一个分区partition的所有数据 Tuple2<String, Tuple2<String, Row>>> iterator
            @Override
            public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> iterator) throws Exception {
                List<SessionDetail> sessionDetailList = new ArrayList<>();
                while (iterator.hasNext()) {
                    Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();

                    Row row = tuple._2._2;
                    // 封装SessionDetail数据
                    SessionDetail sessionDetail = new SessionDetail();
                    sessionDetail.setTaskId(taskId);
                    sessionDetail.setUserId(row.isNullAt(1) ? null : row.getLong(1));
                    sessionDetail.setSessionId(row.isNullAt(2) ? null : row.getString(2));
                    sessionDetail.setPageId(row.isNullAt(3) ? null : row.getLong(3));
                    sessionDetail.setActionTime(row.isNullAt(4) ? null : row.getString(4));
                    sessionDetail.setSearchKeyword(row.isNullAt(5) ? null : row.getString(5));
                    sessionDetail.setClickCategoryId( row.isNullAt(6) ? null : row.getLong(6));
                    sessionDetail.setClickProductId(row.isNullAt(7) ? null : row.getLong(7));
                    sessionDetail.setOrderCategoryIds(row.isNullAt(8) ? null : row.getString(8));
                    sessionDetail.setOrderProductIds(row.isNullAt(9) ? null : row.getString(9));
                    sessionDetail.setPayCategoryIds(row.isNullAt(10) ? null : row.getString(10));
                    sessionDetail.setPayProductIds(row.isNullAt(11) ? null : row.getString(11));
                    sessionDetailList.add(sessionDetail);
                }

                //批量插入
                SessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insertBatch(sessionDetailList);
            }
        });
    }

}
