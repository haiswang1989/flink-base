package com.haiswang.flink.demo.xiaoxiang.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.util.Collector;

/**
 * 其他方式指定keyBy的key
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月27日 下午3:05:00
 */
public class KeyByAnotherTrans extends StreamPro {

    @Override
    protected void run() {
        List<String> lines = Arrays.asList("my name is wang hai sheng wang hai ming");
        DataStream<String> dataStream = env.fromCollection(lines);
        
        /*
        //这段会报错
        //This type (GenericType<com.haiswang.flink.demo.xiaoxiang.transformation.WordCntNoPojo>) cannot be used as key.
        KeyedStream<WordCntNoPojo, Tuple> keyedStream = dataStream.flatMap(new FlatMapFunction<String, WordCntNoPojo>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void flatMap(String line, Collector<WordCntNoPojo> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(new WordCntNoPojo(word, 1));
                }
            }
        }).keyBy("word"); //通过pojo对象的fieldname进行keyby
        
        keyedStream.sum("cnt").print().setParallelism(1); //通过pojo对象的fieldname进行sum
        */
        KeyedStream<WordCntNoPojo, String> keyedStream = dataStream.flatMap(new FlatMapFunction<String, WordCntNoPojo>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void flatMap(String line, Collector<WordCntNoPojo> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(new WordCntNoPojo(word, 1));
                }
            }
        }).keyBy(new KeySelector<WordCntNoPojo, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String getKey(WordCntNoPojo wordCnt) throws Exception {
                return wordCnt.word;
            }
        });
        
        //这边的sum指定参数不知道怎么弄?无法测试成功
        keyedStream.sum("*").print().setParallelism(1);
    }

    public static void main(String[] args) throws Exception {
        KeyByAnotherTrans keyByAnotherTrans = new KeyByAnotherTrans();
        keyByAnotherTrans.init();
        keyByAnotherTrans.run();
        keyByAnotherTrans.start("transformation keyBy another.");
    }
}

/**
 * WordCntNoPojo不是一个POJO对象,不能使用为key
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月27日 下午3:08:38
 */
class WordCntNoPojo {
    public String word;
    public int cnt;
    
    public WordCntNoPojo(String wordArgs, int cntArgs) {
        this.word = wordArgs;
        this.cnt = cntArgs;
    }
    
    @Override
    public String toString() {
        return word  + " : " + cnt;
    }
}
