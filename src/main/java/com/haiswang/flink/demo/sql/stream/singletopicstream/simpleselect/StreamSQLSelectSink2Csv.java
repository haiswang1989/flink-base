package com.haiswang.flink.demo.sql.stream.singletopicstream.simpleselect;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;

import com.haiswang.flink.demo.sql.stream.common.SendSource;

/**
 * sql on datastream 
 * 
 * sink结果数据到CSV文件
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月14日 下午3:04:14
 */
public class StreamSQLSelectSink2Csv {

    @SuppressWarnings("rawtypes")
    public static void main(String[] args) throws Exception {
        
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(streamEnv);
        
        DataStream<Tuple3<Long, String, Integer>> dataStream = streamEnv.addSource(new SendSource());
//        Table sourceTable = tableEnv.fromDataStream(dataStream, "id, name, age");
//        Table result = tableEnv.sqlQuery("select id, name, age from " + sourceTable + " where age <= 34");
        
        //注册表
        tableEnv.registerDataStream("source_table", dataStream, "id, name, age");
//        Table result = tableEnv.sqlQuery("select id, name, age from source_table where age <= 34");
        
        
        String[] fieldNames = {"id", "name", "age"};
        TypeInformation[] fieldTypes = {Types.LONG, Types.STRING, Types.INT};
        
        //sink数据到csv
        TableSink tableSink = new CsvTableSink("E:\\Flink\\sink", ",");
        tableEnv.registerTableSink("result_sink_table", fieldNames, fieldTypes, tableSink);
        tableEnv.sqlUpdate("insert into result_sink_table select id, name, age from source_table where age <= 34");
        
        streamEnv.execute("SQL on stream.");
    }
}
