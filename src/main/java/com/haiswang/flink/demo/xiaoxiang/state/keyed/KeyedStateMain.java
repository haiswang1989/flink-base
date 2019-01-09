package com.haiswang.flink.demo.xiaoxiang.state.keyed;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

import com.haiswang.flink.demo.xiaoxiang.transformation.StreamPro;

/**
 * 事件流Tuple2<EventId, value>, 求不同的事件eventId下, 相邻的3个value的平均值
 * 事件流如下:
 * (1,4), (2,3), (3,1), (1,2), (3,2), (1,2), (2,2), (2,9) 
 * 
 * 事件1：(4 + 2 + 2) / 3
 * 事件2：(3 + 2 + 9) / 3
 * 事件3: 只有2个value,还不满足3个value
 * 
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月7日 下午4:18:15
 */
public class KeyedStateMain extends StreamPro {

    @Override
    protected void run() {
        DataStream<Tuple2<Integer, Integer>> stream = 
                env.fromElements(new Tuple2<Integer, Integer>(1, 4), 
                                new Tuple2<Integer, Integer>(2, 3), 
                                new Tuple2<Integer, Integer>(3, 1), 
                                new Tuple2<Integer, Integer>(1, 2), 
                                new Tuple2<Integer, Integer>(3, 2), 
                                new Tuple2<Integer, Integer>(1, 2), 
                                new Tuple2<Integer, Integer>(2, 2), 
                                new Tuple2<Integer, Integer>(2, 9));
        
        stream.keyBy(0).flatMap(new CountWithKeyedOperatorState()).print().setParallelism(1);
    }

    public static void main(String[] args) throws Exception {
        KeyedStateMain keyedStateMain = new KeyedStateMain();
        keyedStateMain.init();
        keyedStateMain.run();
        keyedStateMain.start("keyed operator state.");
    }
}
