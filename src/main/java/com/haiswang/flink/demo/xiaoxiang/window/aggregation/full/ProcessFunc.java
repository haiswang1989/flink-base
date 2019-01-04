package com.haiswang.flink.demo.xiaoxiang.window.aggregation.full;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.haiswang.flink.demo.xiaoxiang.source.SendSentenceSource;
import com.haiswang.flink.demo.xiaoxiang.transformation.StreamPro;

/**
 * 全量聚合之process()方法
 * WindowedStream的Process方法
 * apply方法以及慢慢的被process方法淘汰了
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月28日 下午5:07:38
 */
public class ProcessFunc extends StreamPro {

    @Override
    protected void run() {
        
        DataStream<String> datastream = env.addSource(new SendSentenceSource());
        datastream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : sentence.split(" ")) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        })
        .keyBy(0)
        .timeWindow(Time.seconds(5l))
        .process(new ProcessWindowFunction<Tuple2<String,Integer>, String, Tuple, TimeWindow>() {
            private static final long serialVersionUID = 1L;
            /**
             * Context是窗口的元数据
             * 内部有很多属性,提供更灵活的操作算子
             */
            @Override
            public void process(Tuple key,
                    ProcessWindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>.Context context,
                    Iterable<Tuple2<String, Integer>> tuples, Collector<String> collect) throws Exception {
                //窗口的上下文,提供了更多的属性
                //context
                int sum = 0;
                for (Tuple2<String, Integer> tuple2 : tuples) {
                    sum += tuple2.f1;
                }
                collect.collect(key + " : " + sum);
            }
        }).print().setParallelism(1);
    }
    
    public static void main(String[] args) throws Exception {
        ProcessFunc processFunction = new ProcessFunc();
        processFunction.init();
        processFunction.run();
        processFunction.start("window process.");
    }
}
