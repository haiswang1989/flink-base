package com.haiswang.flink.demo.sql.stream.common;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.types.Row;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月15日 下午4:25:44
 */
public class SingleFlinkKafkaPartitioner extends FlinkKafkaPartitioner<Row> {

    private static final long serialVersionUID = 1L;

    @Override
    public int partition(Row record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        return 0;
    }
}
