package com.spark.dao;

import com.spark.pojo.Task;
import com.spark.util.JDBCUtils;

import java.sql.ResultSet;

public class TaskDAO {
    public Task findById(long taskid) {
        final Task task = new Task();

        String sql = "select * from task where task_id = ?";
        Object[] params = new Object[]{taskid};

        JDBCUtils jdbcHelper = JDBCUtils.getInstance();
        jdbcHelper.executeQuery(sql, params, new JDBCUtils.QueryCallback() {

            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()) {
                    long taskid = rs.getLong(1);
                    String taskName = rs.getString(2);
                    String createTime = rs.getString(3);
                    String startTime = rs.getString(4);
                    String finishTime = rs.getString(5);
                    String taskType = rs.getString(6);
                    String taskStatus = rs.getString(7);
                    String taskParam = rs.getString(8);

                    task.setTaskId(taskid);
                    task.setTaskName(taskName);
                    task.setCreateTime(createTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParam);
                }
            }

        });

        return task;
    }
}
