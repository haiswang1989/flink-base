package com.haiswang.flink.demo.basicapi;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 
 * 
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月14日 上午10:42:40
 */
public class DemoWithLambda {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.readTextFile("input.txt")
        .filter(line -> line.indexOf("sheng")!=-1)
        /*
        .flatMap((String line, Collector<WC> out) -> {
            for (String word : line.split(" ")) {
                WC wc = new WC();
                wc.word = word;
                wc.count = 1;
                out.collect(wc);
            }
        })
        */
        .flatMap(new FlatMapFunction<String, WC>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void flatMap(String line, Collector<WC> out) throws Exception {
                for (String word : line.split(" ")) {
                    WC wc = new WC();
                    wc.word = word;
                    wc.count = 1;
                    out.collect(wc);
                }
            }
        })
        .keyBy("word")
        .reduce((wc1, wc2) -> {
            WC newWc = new WC();
            newWc.word = wc1.word;
            newWc.count = wc1.count + wc2.count;
            return newWc;
        })
        .print()
        .setParallelism(1);
        env.execute("Demo with lambda");
    }
    
    public static class WC {
        public String word;
        public long count;
        
        public WC() {}
        
        @Override
        public String toString() {
            return word + ":" + count;
        }
    }
}

