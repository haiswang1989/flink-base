package com.haiswang.flink.demo.xiaoxiang.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.util.Collector;

/**
 * keyBy POJO对象的成员
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月27日 下午2:59:14
 */
public class KeyByPojoTrans extends StreamPro {

    @Override
    protected void run() {
        List<String> lines = Arrays.asList("my name is wang hai sheng wang hai ming");
        DataStream<String> dataStream = env.fromCollection(lines);
        
        KeyedStream<WordCnt, Tuple> keyedStream = dataStream.flatMap(new FlatMapFunction<String, WordCnt>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void flatMap(String line, Collector<WordCnt> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(new WordCnt(word, 1));
                }
            }
        }).keyBy("word"); //通过pojo对象的fieldname进行keyby
        
        keyedStream.sum("cnt").print().setParallelism(1); //通过pojo对象的fieldname进行sum
    }
    
    public static void main(String[] args) throws Exception {
        KeyByPojoTrans keyByPojoTrans = new KeyByPojoTrans();
        keyByPojoTrans.init();
        keyByPojoTrans.run();
        keyByPojoTrans.start("transformation keyby pojo.");
    }
}
