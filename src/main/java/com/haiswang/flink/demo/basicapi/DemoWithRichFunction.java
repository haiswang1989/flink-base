package com.haiswang.flink.demo.basicapi;

import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DemoWithRichFunction {

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        
        PassParamToFunction passParamToFunction = new PassParamToFunction();
        passParamToFunction.byNoRichFunction();
        passParamToFunction.byRichFunctionWithDataSet();
        
        AccessBroadcastVariablesByRichFunction accessBroadcastVariablesByRichFunction = new AccessBroadcastVariablesByRichFunction();
        accessBroadcastVariablesByRichFunction.accessBroadcastVariables();
    }

}

/**
 * 通过rich function获取广播变量
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月15日 上午10:13:28
 */
class AccessBroadcastVariablesByRichFunction {
    
    /**
     * 测试正常
     * 
     * @throws Exception
     */
    public void accessBroadcastVariables() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //广播变量
        DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3);
        DataSet<String> data = env.fromElements("a", "b");
        
        data.map(new RichMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void open(Configuration parameters) throws Exception {
                List<Integer> broadcastVariables = getRuntimeContext().getBroadcastVariable("BroadcastVariables");
                System.out.println(broadcastVariables.toString());
            }
            
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).withBroadcastSet(toBroadcast, "BroadcastVariables").setParallelism(1).print();
    }
}

/**
 * 通过Rich Function来传参数
 * 
 * 对DataStream数据结构无效(流式数据)
 * 对DataSet数据结构有效(Batch数据有效)
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月14日 下午5:27:13
 */
class PassParamToFunction {
    
    /**
     * 通过Rich Function给用户自定义的function传递参数,只对DataSet的数据结构有效果
     * 
     * @throws Exception
     */
    public void byRichFunctionWithDataSet() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> elements = env.fromElements(1, 2, 3);
        Configuration config = new Configuration();
        config.setInteger("threshold", 2);
        elements.filter(new RichFilterFunction<Integer>() {
            private static final long serialVersionUID = 1L;
            private int threshold;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                threshold = parameters.getInteger("threshold", 1);
            }
            
            @Override
            public boolean filter(Integer value) throws Exception {
                return value >= threshold;
            }
        }).withParameters(config).setParallelism(1).print();
        
        //对于Dataset结构的数据,不需要持续的执行,所以不需要下面的执行语句
        //env.execute("Pass Param By Rich Function to Dataset");
    }
    
    //////////////////////////////////////////////////////////////////////////////
    /**
     * 直接通过构造函数往用户自定义的Function传递参数
     * 
     * @throws Exception
     */
    public void byNoRichFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("input.txt")
        .flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(word);
                }
            }
        }).filter(new MyFilter(5)).print().setParallelism(1);
        env.execute("ByNoRichFunction");
    }
    
    class MyFilter implements FilterFunction<String> {
        
        private static final long serialVersionUID = 1L;
        private int threshold;
        
        public MyFilter(int thresholdArgs) {
            this.threshold = thresholdArgs;
        }
        
        @Override
        public boolean filter(String word) throws Exception {
            return word.length() >= threshold;
        }
    }
    //////////////////////////////////////////////////////////////////////////////
    
    /**
     * 通过RichFunction给自定义的Function传参数
     * 好像对于DataStream结构没效果
     * 
     * @throws Exception
     */
    public void byRichFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        config.setInteger("threshold", 1);
        //env.getConfig().setGlobalJobParameters(config); //这样设置在open()方法中无法获取
        DataStream<String> texts = env.readTextFile("input.txt");
        //texts.getExecutionConfig().setGlobalJobParameters(config); //这样设置在open()方法中也无法获取
        texts.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(word);
                }
            }
        }).filter(new RichFilterFunction<String>() {
            private static final long serialVersionUID = 1L;
            private int threshold;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                threshold = parameters.getInteger("threshold", 4);
            }
            
            @Override
            public boolean filter(String value) throws Exception {
                return value.length() >= threshold;
            }
        }).print().setParallelism(1);
        env.execute("ByRichFunction");
    }
}