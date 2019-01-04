package com.haiswang.flink.demo.xiaoxiang.transformation.joinandcogroup;

import java.util.Iterator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.haiswang.flink.demo.xiaoxiang.transformation.StreamPro;

/**
 * 
 * 
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月28日 下午3:17:16
 */
public class CogroupTrans extends StreamPro {
    
    @Override
    protected void run() {
        DataStream<Input1> input1Stream = env.addSource(new Input1Source());
        DataStream<Input2> input2Stream = env.addSource(new Input2Source());
        
        input1Stream.coGroup(input2Stream).where(new KeySelector<Input1, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String getKey(Input1 input1) throws Exception {
                return input1.getInput1Uid();
            }
        }).equalTo(new KeySelector<Input2, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String getKey(Input2 input2) throws Exception {
                return input2.getInput2Uid();
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        .apply(new CoGroupFunction<Input1, Input2, Input1JoinInput2>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void coGroup(Iterable<Input1> input1, Iterable<Input2> input2, Collector<Input1JoinInput2> out)
                    throws Exception {
                //join和cogroup最主要的差别就是apply方法的入参
                //join是JoinFunction的子类 ,join()方法入参是单个元素
                //cogroup是CoGroupFunction的子类,coGroup()方法的入参数一个迭代器
                //cogroup的扩展性比较强 ,join + group by + order by实现比较容易
                System.out.println("input1 input2-------------------------");
                Iterator<Input1> input1Iterator = input1.iterator();
                while(input1Iterator.hasNext()) {
                    System.out.println(input1Iterator.next());
                }
                
                Iterator<Input2> input2Iterator = input2.iterator();
                while(input2Iterator.hasNext()) {
                    System.out.println(input2Iterator.next());
                }
            }
        }).print().setParallelism(1);
        
    }

    public static void main(String[] args) throws Exception {
        CogroupTrans cogroupTrans = new CogroupTrans();
        cogroupTrans.init();
        cogroupTrans.run();
        cogroupTrans.start("transformation cogroup.");
    }
}
