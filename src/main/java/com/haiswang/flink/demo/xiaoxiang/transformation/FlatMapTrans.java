package com.haiswang.flink.demo.xiaoxiang.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 使用场景：ETL,数据清洗, 数据格式化
 * 操作结果：一对多, 一个输入可以输出0或者多个数据,这样就需要调整下一个算子的并行度了
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月27日 上午10:20:39
 */
public class FlatMapTrans {
    
    public static void main(String[] args) throws Exception {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        List<String> names = Arrays.asList("wang hai sheng wang hai ming");
        DataStream<String> nameStream = env.fromCollection(names);
        
        nameStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }).keyBy(0)/*.timeWindow(Time.seconds(5), Time.seconds(2))*/.sum(1).print().setParallelism(1);
        
        
        env.execute("transformation flatmap.");
    }
}
