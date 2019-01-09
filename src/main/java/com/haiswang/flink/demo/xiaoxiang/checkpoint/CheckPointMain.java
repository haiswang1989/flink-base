package com.haiswang.flink.demo.xiaoxiang.checkpoint;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;

import com.haiswang.flink.demo.xiaoxiang.transformation.StreamPro;

/**
 * checkpoint常见参数设置
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月8日 下午1:59:35
 */
public class CheckPointMain extends StreamPro {

    @Override
    protected void run() {
        //打开checkpoint
        //checkpoint是周期性的
        //60000MS就是checkpoint的执行周期
        env.enableCheckpointing(60000L);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //checkpoint的model
        //CheckpointingMode.EXACTLY_ONCE 和 CheckpointingMode.AT_LEAST_ONCE
        //区别就是在进行checkpoint的时候是否需要进行对齐
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint超时时间,checkpoint 8s还没有完成,就直接超时
        checkpointConfig.setCheckpointTimeout(8000L);
        //两次checkpoint最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(30000L);
        //同一时刻最多有多少个checkpoint进程在执行,默认是1
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        //checkpoint cancel以后是否需要把checkpoint删除
        checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
    }

    public static void main(String[] args) {
        
    }
}
