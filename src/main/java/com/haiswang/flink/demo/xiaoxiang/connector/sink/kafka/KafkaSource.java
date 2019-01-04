package com.haiswang.flink.demo.xiaoxiang.connector.sink.kafka;

import java.util.Properties;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月4日 下午3:18:53
 */
public class KafkaSource {

    public static void main(String[] args) {
        
        String topic = "";
        
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "");
        props.setProperty("group.id", "flink_kafka_consumer");
        
        DeserializationSchema<String> valueDeserializer = new SimpleStringSchema();
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, valueDeserializer, props);
    }
}
