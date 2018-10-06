### 用户访问Session分析:

目的:

1. 可以根据使用者指定的某些条件, 筛选出指定的一些用户(有特定年龄, 职业, 城市);
2. 对这些用户在指定的日期范围内发起的 session, 进行聚合统计, 比如, 统计出访问时长在 0~3s 之间的 session 占总访问 session 数量的比例;
3. 按照时间比例, 比如一天有24个小时, 其中 12:00 - 13:00 的 session 数量占当天总 session 数量的 50%, 当天总 session 的数量为 10000 个, 那么当天总共要出去 1000 个 session, 12:00 - 13:00 之间的用户, 就得抽取 500 个, 而且这 500 个需要随机抽取;
4. 获取点击量, 下单量和支付量都排名前 10 的商品种类;
5. 获取 top10 的商品种类的点击数量排名前 10 的 session;

> ##### 在做任何大数据系统/平台项目, 第一步要做的就是数据调研, 也就是分析平台基于的底层的基础数据, 分析表结构, 明白表之间的关系, 表中数据的更新粒度, 是一天更新一次还是一个小时更新一次, 会不会有脏数据, 什么时候更新等问题.

### 表的设计:

表名: **user_visit_action** (模拟存放网站, app 每天点击流的数据)( hive 表)

```sql
user_visit_action(
	date '日期, 代表这个用户点击行为在那一天发生的',
	user_id   '代表这个点击行为属于哪一个用户',
	session_id   '唯一标志某个用户的一个访问 session',
	page_id   '点击了某个商品/品类, 也肯能是搜索了某个关键词, 然后进入了某个页面, 页面的 id',
	action_time   '这个点击行为发生的时间',
	search_keyword string  '如果用户执行了一个搜索行为, 搜索的关键词',
	click_category_id string  '可能是网站首页, 点击了某个品类(手机, 电脑等)',
	click_product_id string  '网站首页或者商品列表, 点击了某个商品',
	order_category_ids string  '代表了用户将某些商品加入了购物车, 然后一次性对购物车中的商品下了一个订单, 这就代表了某次下单的行为中, 有哪些商品品类',
	order_product_ids string  '某次下单, 具体对那些商品下的订单',
    pay_category_ids string  '对某个订单或者多个订单, 进行了一次支付的行为, 对应了那些品类',
    pay_product_ids string  '支付行为下, 对应那些具体的商品'
)
```

表名: **user_info** (普通的用户基础信息表, 放置了网站/app所用注册用户的信息) (hive 表)

```sql
user_info(
	user_id   '每一个用户的唯一标识, 自增长',
	username   '用户登录名',
	name string  '用户自己的昵称或者真是姓名',
	age string  '用户的年龄',
	professional   '用户的职业',
	city string  '用户所在城市'
)
```

表名 : **task** (MySQL 表)

​	用来保存平台的使用者, 通过 J2EE 系统, 提交的基于特定筛选参数的分析任务的信息, 就会通过 J2EE 系统保存到 task 表中. 设计为 MySQL 表的原因是因为 J2EE 系统是要实时的进行插入和查询.

```sql
task(
	task_id   '表的主键',
	task_name   '任务名称',
	create_time   '创建时间',
	start_time   '开始运行的时间',
	finish_time   '结束运行的时间',
	task_type  '任务类型, 在大数据平台中, 会有不同的类型的统计分析',
	task_status   '任务状态',
	task_param   '使用 Json 格式封装用户提交的任务对应的特殊的筛选参数'
)
```

#### 需求分析:

1. 按条件筛选 session;

   ```java
   	按照特定的条件筛选出对应的用户群体, 比如特定的年龄范围, 职业, 在某个时间段, 某个城市访问的用户发起的 session.
   	可以通过筛选, 对感兴趣的用户群体进行复杂的业务逻辑分析统计, 最终的结果也是针对特定群体的分析结果, 而不是基于所有用户的泛泛的分析结果.
   ```

2. 统计出符合条件的 session 中, 访问时长 1s-3s, 4s-6s, 7s-9s, 10s-30s, 30s-60s, 1m-3m, 3m-10m, 10m-30m, 30m 以上各个范围内的 session 占比, 访问步长在 1-3, 4-6, 7-9, 10-30, 30-60, 60以上各个范围内的 session 占比;

   ```java
   	这一步的统计结果可以从全局的结果观察到, 符合条件的用户在访问浏览商品时的一些行为习惯, 比如对于大多数人在访问时或停留多长时间, 在一次会话中, 会访问多少个页面等.
   ```

3. 在符合条件的 session 中, 按照时间比例随机抽取 1000 个session;

   ```java
   	可以根据符合条件的 session, 根据时间比例均匀的采样一定的 session, 然后观察每个 session 具体的点击流/行为, 比如先进入了首页, 点击了某类商品, 然后下单, 支付等.
   ```

4. 在符合条件的 session 中, 获取点击, 下单, 支付数量排名前 10  的品类;

   ```java
   	这个功能很重要, 可以分析对于特定的用户群体, 他们最感兴趣的商品的品类, 可以清晰的观察到不同层次, 类型的用户群体的心里和喜好.
   ```

5. 对排名前 10 的品类中, 分别获取其点击次数排名前 10 的 session;

   ```java
   	可以根据统计的结果分析, 对于某个用户群体最感兴趣的品类, 各个品类中最感兴趣的的典型的用户的 session 的行为.
   ```

#### 使用spark开发原则:

1. 尽量少生成 RDD
2. 尽量少对RDD进行算子操作, 如果可以尽量在一个算子中去实现多个功能
3. 尽量少对RDD使用shuffle算子的操作, 例如groupbykey, reducebykey, sortbykey等, shuffle操作会导致大量的磁盘读写, 降低计算性能, 还可能会导致数据倾斜的问题

#### 数据设计:

1. 根据上游数据(基础数据), 考虑是否需要针对其进行 hive ETL 的开发, 对数据进行进一步的转化, 处理, 便于 spark 作业.
2. 设计 spark 作业要保存的数据的业务表的结构, 可以让平台使用业务表中的数据, 进行结果的展示等操作.

表一: session_aggr_stat, 存储第一个功能, session 聚合统计的结果

```sql
CREATE TABLE session_aggr_stat(
	`task_id` INT(11) NOT NULL COMMENT '标识数据是哪一个task计算的结果',
	`session_count` INT(11) DEFAULT NULL COMMENT '',
	`1s_3s` DOUBLE DEFAULT NULL COMMENT '',
	`14_6s` DOUBLE DEFAULT NULL COMMENT '',
	`7s_9s` DOUBLE DEFAULT NULL COMMENT '',
	`10s_30s` DOUBLE DEFAULT NULL COMMENT '',
	`30s_6s` DOUBLE DEFAULT NULL COMMENT '',
	`1m_3m` DOUBLE DEFAULT NULL COMMENT '',
	`3m_10m` DOUBLE DEFAULT NULL COMMENT '',
	`10m_30m` DOUBLE DEFAULT NULL COMMENT '',
	`30m` DOUBLE DEFAULT NULL COMMENT '',
	`1_3` DOUBLE DEFAULT NULL COMMENT '',
	`4_6` DOUBLE DEFAULT NULL COMMENT '',
	`7_9` DOUBLE DEFAULT NULL COMMENT '',
	`10_30` DOUBLE DEFAULT NULL COMMENT '',
	`30_60` DOUBLE DEFAULT NULL COMMENT '',
	`60` DOUBLE DEFAULT NULL COMMENT '',
	PRIMARY KEY (task_id)
)ENGINE=INNODB DEFAULT CHARSET=utf8;
```

表二: session_random_extract, 存储按照时间比例随机抽取功能抽取出来的 1000 个 session

```sql
create table session_random_extract(
	task_id int(11) not null,
	session_id varchar(255) default null,
	start_time varchar(50) default null,
	end_time varchar(50) default null,
	search_keywords varchar(255) default null,
	primary key (task_id)
)ENGINE=InnoDB default charset=utf8;
```

表三: top10_category, 存储按点击, 下单, 支付排序出来的 top10 品类数据

```sql
create table top10_category(
	task_id int(11) not null,
	category_id int(11) default null,
	click_count int(11) default null,
	order_count int(11) default null,
	pay_count int(11) default null,
	primary key (task_id)
)ENGINE=InnoDB default charset=utf8;
```

表四: top10_category_session, 存储 top10 每个品类点击 top10 的 session

```sql
create table top10_category_session (
	task_id int(11) not null,
	category_id int(11) default null,
	session_id varchar(255) default null,
	click_count int(11) default null,
	primary key (task_id)
)ENGINE=InnoDB default charset=utf8;
```

表五: session_detail, 存储随机抽取出来的 session 的明细数据, top10 品类的 session 的明细数据

```sql
create table session_detail (
	task_id int(11) not null,
	user_id int(11) default null,
	session_id varchar(255) default null,
	page_id int(11) default null,
    action_time varchar(255) default null,
    search_keyword varchar(255) default null,
    click_category_id int(11) default null,
    click_product_id int(11) default null,
    order_category_ids varchar(255) default null,
    order_product_ids varchar(255) default null,
    pay_category_ids varchar(255) default null,
    pay_product_ids varchar(255) default null,
	primary key (task_id)
)ENGINE=InnoDB default charset=utf8;
```

表六: task, 存储网站插入任务的信息

```sql
create table task(
	task_id int(11) not null auto_increment,
	task_name varchar(255) default null,
	create_time varchar(255) default null,
	start_time varchar(255) default null,
	finish_time varchar(255) default null,
	task_type varchar(255) default null,
	task_status varchar(255) default null,
	task_param text,
	primary key (task_id)
)ENGINE=InnoDB default charset=utf8;
```

#### spark 开发版本:

```java
spark 2.2.0
jdk 1.8
```

使用到的技术点:

```java
聚合统计, 随机抽取, 分组topN, 二次排序, 自定义Accumulator, 性能调优, troubleshooting, 数据倾斜解决方案
```
