package com.haiswang.flink.demo.sql.stream.singletopicstream.simpleselect;

import java.util.Optional;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import com.haiswang.flink.demo.sql.stream.common.SendSource;
import com.haiswang.flink.demo.sql.stream.common.SingleFlinkKafkaPartitioner;
import com.haiswang.flink.demo.sql.stream.common.SingleSerializationSchema;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月14日 下午3:04:14
 */
public class StreamSQLSelectSink2Kafka {

    @SuppressWarnings("rawtypes")
    public static void main(String[] args) throws Exception {
        
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(streamEnv);
        
        //source
        DataStream<Tuple3<Long, String, Integer>> dataStream = streamEnv.addSource(new SendSource());
        
        //注册表
        tableEnv.registerDataStream("source_table", dataStream, "id, name, age");
        
        //schema
        String[] fieldNames = {"id", "name", "age"};
        TypeInformation[] fieldTypes = {Types.LONG, Types.STRING, Types.INT};
        TableSchema schema = new TableSchema(fieldNames, fieldTypes);
        
        //topic
        String topic = "flink_table_sink_brother";
        
        //kafka properties
        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("bootstrap.servers", "192.168.56.101:9092");
        kafkaProp.setProperty("group.id", "SinkGroup");
        
        FlinkKafkaPartitioner<Row> partitioner = new SingleFlinkKafkaPartitioner();
        Optional<FlinkKafkaPartitioner<Row>> optionalPartitioner = Optional.ofNullable(partitioner);
        SerializationSchema<Row> serializationSchema = new SingleSerializationSchema();
        
        //sink到kafka
        TableSink<Row> kafkaSink = new KafkaTableSink(schema, topic, kafkaProp, optionalPartitioner, serializationSchema);
        
        tableEnv.registerTableSink("result_sink_table", kafkaSink);
        tableEnv.sqlUpdate("insert into result_sink_table select id, name, age from source_table where age <= 34");
        
        streamEnv.execute("SQL on stream.");
    }
}
