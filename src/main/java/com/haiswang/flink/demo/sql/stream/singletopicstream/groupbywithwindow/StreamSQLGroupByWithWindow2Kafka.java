package com.haiswang.flink.demo.sql.stream.singletopicstream.groupbywithwindow;

import java.sql.Timestamp;
import java.util.Optional;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import com.haiswang.flink.demo.sql.stream.common.SendSourceWithEventTime;
import com.haiswang.flink.demo.sql.stream.common.SingleFlinkKafkaPartitioner;
import com.haiswang.flink.demo.sql.stream.common.SingleSerializationSchema;

/**
 * 对于这个sql是来一条数据计算一次
 * 
 * select id, name, count(1) as cnt from source_table group by id,name
 * 
 * GroupBy on a streaming table produces an updating result.
 * 对于group by操作,生成的流不是appendstream(只有insert语句)
 * 而是retractStream (delete, insert)
 * 
 * flink stream分为三种：
 * 1：Append-only stream (单纯的insert)
 * 2：Retract stream (delete insert)
 * 3：Upsert stream (insert upsert)
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月15日 下午5:30:34
 */
public class StreamSQLGroupByWithWindow2Kafka {

    @SuppressWarnings("rawtypes")
    public static void main(String[] args) throws Exception {
        
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(streamEnv);
        //source
        DataStream<Tuple4<Long, String, Integer, Timestamp>> dataStream = streamEnv.addSource(new SendSourceWithEventTime());
        //注册表
        tableEnv.registerDataStream("source_table", dataStream, "id, name, age, eventtime");
        
        
        //schema, 这边多定义了一个'c1'字段,类型是String,用于存储datastream中流的类型的
        String[] fieldNames = {"id", "name", "cnt", "c1"};
        TypeInformation[] fieldTypes = {Types.LONG, Types.STRING, Types.LONG, Types.STRING};
        TableSchema schema = new TableSchema(fieldNames, fieldTypes);
        //topic
        String topic = "flink_table_sink_groupby";
        //kafka properties
        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("bootstrap.servers", "192.168.56.101:9092");
        kafkaProp.setProperty("group.id", "SinkGroup");
        //kafka sink相关参数
        FlinkKafkaPartitioner<Row> partitioner = new SingleFlinkKafkaPartitioner();
        Optional<FlinkKafkaPartitioner<Row>> optionalPartitioner = Optional.ofNullable(partitioner);
        SerializationSchema<Row> serializationSchema = new SingleSerializationSchema();
        //自定义sink的把retract stream, sink到kafka
        //RetractStreamKafkaSink retractStreamKafkaSink = new RetractStreamKafkaSink(schema, topic, kafkaProp, optionalPartitioner, serializationSchema);
        
        TableSink<Row> kafkaSink = new KafkaTableSink(schema, topic, kafkaProp, optionalPartitioner, serializationSchema);
        tableEnv.registerTableSink("result_sink_table", kafkaSink);
        
        //上边多定义了一个'c1'字段,类型是String,用于存储datastream中流的类型的,这边的SQL中就要添加一个"占位"字段
        tableEnv.sqlUpdate("insert into result_sink_table select id, name, count(1) as cnt, 'true' from source_table group by TUMBLE(eventtime, INTERVAL '5' SECOND),id,name");
        streamEnv.execute("SQL on stream.");
    }
}
