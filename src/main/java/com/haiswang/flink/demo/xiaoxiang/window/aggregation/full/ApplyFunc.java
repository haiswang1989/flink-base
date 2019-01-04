package com.haiswang.flink.demo.xiaoxiang.window.aggregation.full;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.haiswang.flink.demo.xiaoxiang.source.SendSentenceSource;
import com.haiswang.flink.demo.xiaoxiang.transformation.StreamPro;

/**
 * 全量聚合之apply()方法
 * 
 * WindowedStream的Apply方法
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月28日 下午5:07:14
 */
public class ApplyFunc extends StreamPro {

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
        .apply(new WindowFunction<Tuple2<String,Integer>, String, Tuple, TimeWindow>() {
            private static final long serialVersionUID = 1L;
            public void apply(Tuple key, TimeWindow window, Iterable<Tuple2<String,Integer>> input, Collector<String> out) throws Exception {
                int sum = 0;
                for (Tuple2<String, Integer> tuple2 : input) {
                    sum += tuple2.f1;
                }
                
                out.collect(key + " : " + sum);
            }
        }).print().setParallelism(1);
    }

    public static void main(String[] args) throws Exception {
        ApplyFunc applyFunction = new ApplyFunc();
        applyFunction.init();
        applyFunction.run();
        applyFunction.start("window apply function.");
    }
}
