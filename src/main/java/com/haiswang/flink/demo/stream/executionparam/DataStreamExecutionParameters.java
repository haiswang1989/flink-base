package com.haiswang.flink.demo.stream.executionparam;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 设置Flink运行时的一些参数
 * 
 * 常见的参数设置以及默认值
 * https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/execution_configuration.html
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月19日 上午10:18:27
 */
public class DataStreamExecutionParameters {

    public static void main(String[] args) {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        //StreamExecutionEnvironment定义了ExecutionConfig用于设置"运行时"阶段的参数
        ExecutionConfig config = env.getConfig();
        long watermark = config.getAutoWatermarkInterval();
        System.out.println("watermark : " + watermark);
    }

}
