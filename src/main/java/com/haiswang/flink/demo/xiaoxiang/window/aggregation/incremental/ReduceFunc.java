package com.haiswang.flink.demo.xiaoxiang.window.aggregation.incremental;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.haiswang.flink.demo.xiaoxiang.source.SendSentenceSource;
import com.haiswang.flink.demo.xiaoxiang.transformation.StreamPro;

/**
 * 增量聚合之reduce()方法
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月2日 上午10:44:56
 */
public class ReduceFunc extends StreamPro{
    
    @Override
    protected void run() {
        DataStream<String> sentenceStream = env.addSource(new SendSentenceSource());
        
        sentenceStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : sentence.split(" ")) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        })
        .keyBy(0)
        .timeWindow(Time.seconds(5))
        .reduce(new ReduceFunction<Tuple2<String,Integer>>() { //来一条数据处理一下,增量处理
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2)
                    throws Exception {
                return new Tuple2<String, Integer>(value1.f0, value1.f1 + value2.f1);
            }
        }).print().setParallelism(1);
    }
    
    public static void main(String[] args) throws Exception {
        ReduceFunc reduceFunc = new ReduceFunc();
        reduceFunc.init();
        reduceFunc.run();
        reduceFunc.start("window reduce funtion.");
    }

}
