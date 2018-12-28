package com.haiswang.flink.demo.xiaoxiang.transformation.joinandcogroup;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.haiswang.flink.demo.xiaoxiang.transformation.StreamPro;

/**
 * 两个流的JOIN
 * 
 * 时间窗口内
 * 
 * 实时 和 离线的结合
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月27日 下午3:33:08
 */
public class JoinTrans extends StreamPro {

    @Override
    protected void run() {
        
        DataStream<Input1> input1Stream = env.addSource(new Input1Source());
        DataStream<Input2> input2Stream = env.addSource(new Input2Source());
        
        //input1Stream.print().setParallelism(1);
        //input2Stream.print().setParallelism(1);
        
        /*
         * 1 ,i1
         * 2 ,i2
         * 3 ,i3
         * 3 ,i4
         * 
         * 1 ,ii1
         * 1 ,ii11
         * 2 ,ii2
         * 3 ,ii3
         * 
         * 1 ,i1 ,ii1
         * 1 ,i1 ,ii11
         * 2 ,i2 ,ii2
         * 3 ,i3 ,ii3
         * 3 ,i4 ,ii4
         * 
         * 
         */
        // input1Stream join input2Stream on input1Stream.input1Uid=input2Stream.input2Uid
        input1Stream.join(input2Stream).where(new KeySelector<Input1, String>() {
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
        .apply(new JoinFunction<Input1, Input2, Input1JoinInput2>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Input1JoinInput2 join(Input1 input1, Input2 input2) throws Exception {
                //input1的uid和input2的uid肯定是一致的
                System.out.println(input1 + ":" + input2);
                return new Input1JoinInput2(input1, input2);
            }
        }).print().setParallelism(1);
    }

    public static void main(String[] args) throws Exception {
        JoinTrans joinTrans = new JoinTrans();
        joinTrans.init();
        joinTrans.run();
        joinTrans.start("transformation join.");
    }
}
