package com.haiswang.flink.demo.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 从socket流中统计单词
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月16日 上午10:20:26
 */
public class DataStreamSocketWordCount {

    public static void main(String[] args) {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dStream = env.socketTextStream("192.168.56.101", 9999);
        
        DataStream<Tuple2<String, Integer>> windowSum = 
                dStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String word : line.split(" ")) {
                            out.collect(new Tuple2<String, Integer>(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5)) //翻滚的时间窗口(每5秒打印近5秒的统计数据)
                //.timeWindow(Time.seconds(5), Time.seconds(2)) //滑动时间窗口(每2秒打印近5秒的统计数据)
                .sum(1);
        
        windowSum.print().setParallelism(1);
        try {
            env.execute("Data Stream window word sum");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
