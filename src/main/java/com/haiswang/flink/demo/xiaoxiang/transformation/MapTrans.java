package com.haiswang.flink.demo.xiaoxiang.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用场景：ETL,数据清洗, 数据格式化
 * 操作结果：一对一
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月27日 上午10:13:08
 */
public class MapTrans {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<String> names = Arrays.asList("wanghaisheng", "wanghaiming");
        DataStream<String> nameStream = env.fromCollection(names);
        nameStream.map(new MapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String map(String value) throws Exception {
                if("wanghaisheng".equals(value)) {
                    return "wuxiaotian";
                } else if("wanghaiming".equals(value)) {
                    return "liuxiaolan";
                }
                return null;
            }
        }).print().setParallelism(1);
        
        env.execute("transformation map.");
    }

}
