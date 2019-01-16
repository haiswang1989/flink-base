package com.haiswang.flink.demo.sql.stream.common;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月15日 下午5:01:25
 */
public class SingleSerializationSchema implements SerializationSchema<Row> {
    
    private static final long serialVersionUID = 1L;

    @Override
    public byte[] serialize(Row element) {
        return element.toString().getBytes();
    }
}
