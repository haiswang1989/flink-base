package com.haiswang.flink.demo.xiaoxiang.state.operator;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

import com.haiswang.flink.demo.xiaoxiang.transformation.StreamPro;

/**
 * 假设事件场景为某业务事件流中有事件 1、2、3、4、5、6、7、8、9 ......
 * 现在，想知道两次事件1之间，一共发生了多少次其他的事件，分别是什么事件，然后输出相应结果。
 * 如下:
 * 事件流 1 2 3 4 5 1 3 4 5 6 7 1 4 5 3 9 9 2 1 ....
 * 输出  
 * 4     2 3 4 5
 * 5     3 4 5 6 7
 * 8     4 5 3 9 9 2
 * 
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月7日 下午3:05:48
 */
public class OperatorStateMain extends StreamPro {

    @Override
    protected void run() {
        env.enableCheckpointing(60000);
        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        checkpointConf.setMinPauseBetweenCheckpoints(30000L);
        checkpointConf.setCheckpointTimeout(10000L);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        
        DataStream<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 1, 3, 4, 5, 6, 7, 1, 4, 5, 3, 9, 9, 2, 1);
        
        //注意这边的并行度 , stream -> flatmap的并行度设置为1,不然会乱掉
        stream.flatMap(new CountWithFlatMapOperatorState()).setParallelism(1).print().setParallelism(1);
    }
    
    public static void main(String[] args) throws Exception {
        OperatorStateMain operatorStateMain = new OperatorStateMain();
        operatorStateMain.init();
        operatorStateMain.run();
        operatorStateMain.start("operator state.");
    }
}
