package com.haiswang.flink.demo.xiaoxiang.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * 数据过滤
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月27日 上午10:26:11
 */
public class FilterTrans extends StreamPro {

    @Override
    protected void run() {
        DataStream<Integer> intStream = env.fromElements(1, 2, 3, 4);
        intStream.filter(new FilterFunction<Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean filter(Integer value) throws Exception {
                return value % 2 == 0;
            }
        }).print().setParallelism(1);
    }
    
    public static void main(String[] args) throws Exception {
        FilterTrans filterTrans = new FilterTrans();
        filterTrans.init();
        filterTrans.run();
        filterTrans.start("transformation filter.");
    }

}
