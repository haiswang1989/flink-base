package com.haiswang.flink.demo.xiaoxiang.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.util.Collector;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月27日 上午11:26:56
 */
public class KeyByTupleTrans extends StreamPro {
    
    @Override
    protected void run() {
        List<String> lines = Arrays.asList("my name is wang hai sheng wang hai ming");
        DataStream<String> dataStream = env.fromCollection(lines);
        
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }).keyBy(0)/*.keyBy("f0")*/;
        //keyBy(0) 和 keyBy("f0")效果是一样的, 0是指位置,f0是指第一个元素的名称
        
        keyedStream.sum(1).print().setParallelism(1);
    }
    
    public static void main(String[] args) throws Exception {
        KeyByTupleTrans keyByTrans = new KeyByTupleTrans();
        keyByTrans.init();
        keyByTrans.run();
        keyByTrans.start("transformation keyby tuple.");
    }
}


