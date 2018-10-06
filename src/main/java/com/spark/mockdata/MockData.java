package com.spark.mockdata;

import com.spark.util.DateUtils;
import com.spark.util.MyStringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;


import java.util.*;

/**
 * 生成模拟数据
 */
public class MockData {

    public static void mock(JavaSparkContext sc, SparkSession spark) {
        List<Row> rows = new ArrayList<Row>();
        //ArraySeq<Row> rows = new ArraySeq<Row>(20);
        /**
         * =================================模拟 user_visit_action 表数据=================================
         */
        // 模拟用户搜搜的关键字
        String[] searchKeywords = new String[]{"火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
                "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"};
        // 模拟当前时间
        String date = DateUtils.getTodayDate();
        // 模拟用户浏览的行为
        String[] actions = new String[]{"search", "click", "order", "pay"};
        Random random = new Random();

        // 随机生成 100 个用户id
        for (int i = 0; i < 100; i++) {
            long userid = random.nextInt(100);

            // 每个用户模拟10个会话
            for (int j = 0; j < 10; j++) {
                String sessionid = UUID.randomUUID().toString().replace("-", "");
                String baseActionTime = date + " " + random.nextInt(23);

                // 每次会话的点击行为在 0-100 之间
                for (int k = 0; k < random.nextInt(100); k++) {
                    long pageid = random.nextInt(10);
                    String actionTime = baseActionTime + ":" + MyStringUtils.fulfuill(String.valueOf(random.nextInt(59)
                    )) + ":" + MyStringUtils.fulfuill(String.valueOf(random.nextInt(59)));
                    String searchKeyword = null;
                    Long clickCategoryId = null;
                    Long clickProductId = null;
                    String orderCategoryIds = null;
                    String orderProductIds = null;
                    String payCategoryIds = null;
                    String payProductIds = null;

                    String action = actions[random.nextInt(4)];
                    if ("search".equals(action)) {
                        searchKeyword = searchKeywords[random.nextInt(10)];
                    } else if ("click".equals(action)) {
                        clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));
                        clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));
                    } else if ("order".equals(action)) {
                        orderCategoryIds = String.valueOf(random.nextInt(100));
                        orderProductIds = String.valueOf(random.nextInt(100));
                    } else if ("pay".equals(action)) {
                        payCategoryIds = String.valueOf(random.nextInt(100));
                        payProductIds = String.valueOf(random.nextInt(100));
                    }
                    // 通过 Row Factory 工厂类创建每一个 Row
                    Row row = RowFactory.create(date, userid, sessionid,
                            pageid, actionTime, searchKeyword,
                            clickCategoryId, clickProductId,
                            orderCategoryIds, orderProductIds,
                            payCategoryIds, payProductIds);
                    rows.add(row);
                }
            }
        }

        // 生成 RDD 数据
        JavaRDD<Row> rowsRDD = sc.parallelize(rows);

        // 设置模式
        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("session_id", DataTypes.StringType, true),
                DataTypes.createStructField("page_id", DataTypes.LongType, true),
                DataTypes.createStructField("action_time", DataTypes.StringType, true),
                DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
                DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
                DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
                DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true)));

        //创建 DateFrame 数据
        Dataset<Row> df = spark.createDataFrame(rowsRDD, schema);

        //注册成临时表
        df.createOrReplaceTempView("user_visit_action");
        df.show(10);

        /**
         * =================================模拟 user_info 表数据=================================
         */
        rows.clear();
        String[] sexes = new String[]{"male", "female"};
        for (int i = 0; i < 100; i++) {
            long userid = i;
            String username = "user" + i;
            String name = "name" + i;
            int age = random.nextInt(60);
            String professional = "p" + random.nextInt(100);
            String city = "city" + random.nextInt(100);
            String sex = sexes[random.nextInt(2)];

            // 通过 Row Factory 工厂类创建每一个 Row
            Row row = RowFactory.create(userid, username, name, age,
                    professional, city, sex);
            rows.add(row);
        }

        //生成对应的 RDD
        rowsRDD = sc.parallelize(rows);

        StructType schema2 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("professional", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true)));

        Dataset<Row> df2 = spark.createDataFrame(rowsRDD, schema2).toDF();
        //打印
        df2.show(10);

        df2.createOrReplaceTempView("user_info");
    }
}
