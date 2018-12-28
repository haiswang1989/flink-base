package com.haiswang.flink.demo.table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年10月26日 下午4:58:30
 */
public class TableApi {

    public static void main(String[] args) {
        TableApi tableApi = new TableApi();
        tableApi.batchTableApi();
    }
    
    public void batchTableApi() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        
        DataSet<Tuple3<Integer, Long, String>> ds = testDataSet(env);
        
        Expression xExpression = ExpressionParser.parseExpression("x");
        Expression yExpression = ExpressionParser.parseExpression("y");
        Expression zExpression = ExpressionParser.parseExpression("z");
        
        tableEnv.registerDataSetInternal("table1", ds, new Expression[]{xExpression, yExpression, zExpression});
        
        String sqlQuery = "select x, y, z from table1";
        Table result = tableEnv.sqlQuery(sqlQuery);
        System.out.println(result);
    }
    
    public DataSet<Tuple3<Integer, Long, String>> testDataSet(ExecutionEnvironment env) {
        List<Tuple3<Integer, Long, String>> data = new ArrayList<>();
        data.add(new Tuple3<>(1, 1L, "Hi"));
        data.add(new Tuple3<>(2, 2L, "Hello"));
        data.add(new Tuple3<>(3, 2L, "Hello world"));
        data.add(new Tuple3<>(4, 3L, "Hello world, how are you?"));
        data.add(new Tuple3<>(5, 3L, "I am fine."));
        data.add(new Tuple3<>(6, 3L, "Luke Skywalker"));
        data.add(new Tuple3<>(7, 4L, "Comment#1"));
        data.add(new Tuple3<>(8, 4L, "Comment#2"));
        data.add(new Tuple3<>(9, 4L, "Comment#3"));
        data.add(new Tuple3<>(10, 4L, "Comment#4"));
        data.add(new Tuple3<>(11, 5L, "Comment#5"));
        data.add(new Tuple3<>(12, 5L, "Comment#6"));
        data.add(new Tuple3<>(13, 5L, "Comment#7"));
        data.add(new Tuple3<>(14, 5L, "Comment#8"));
        data.add(new Tuple3<>(15, 5L, "Comment#9"));
        data.add(new Tuple3<>(16, 6L, "Comment#10"));
        data.add(new Tuple3<>(17, 6L, "Comment#11"));
        data.add(new Tuple3<>(18, 6L, "Comment#12"));
        data.add(new Tuple3<>(19, 6L, "Comment#13"));
        data.add(new Tuple3<>(20, 6L, "Comment#14"));
        data.add(new Tuple3<>(21, 6L, "Comment#15"));

        Collections.shuffle(data);
        return env.fromCollection(data);
    }

}
