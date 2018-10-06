package com.spark.daofactory;

import com.spark.dao.*;

/**
 * DAO工厂方法
 */
public class DAOFactory {

    // 获取 TaskDAO 实例对象
    public static TaskDAO getTaskDAO() {
        return new TaskDAO();
    }

    // 获取 SessionAggrStatDAO 实例对象
    public static SessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAO();
    }

    // 获取 SessionRandomExtractDAO 实例对象
    public static SessionRandomExtractDAO getSessionRandomExtractDAO() {
        return new SessionRandomExtractDAO();
    }

    // 获取 SessionDetailDAO 实例对象
    public static SessionDetailDAO getSessionDetailDAO() {
        return new SessionDetailDAO();
    }

    // 获取 Top10CategoryDAO 实例对象
    public static Top10CategoryDAO getTop10CategoryDAO() {
        return new Top10CategoryDAO();
    }

    // 获取 Top10SessionDAO 实例对象
    public static Top10SessionDAO getTop10SessionDAO() {
        return new Top10SessionDAO();
    }
}
