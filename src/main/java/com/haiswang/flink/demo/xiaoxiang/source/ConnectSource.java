package com.haiswang.flink.demo.xiaoxiang.source;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * 自定义的source
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月26日 下午5:45:06
 */

/*
 * ParallelSourceFunction:
 * 1：并行度需要很多,大于1时需要使用ParallelSourceFunction
 * 2：状态管理
 * 
 * SourceFunction
 * 1：并发度为1
 */
public class ConnectSource implements ParallelSourceFunction<Record> /*SourceFunction<Record>*/ {
    
    private static final long serialVersionUID = 1L;

    @Override
    public void run(SourceContext<Record> ctx)
            throws Exception {
        
        while(true) {
            
            Random random = new Random(100l);
            for(int i=0; i<4; i++) {
                Record record = new Record();
                record.setBizName("" + i);
                record.setBizId(i);
                record.setAttr(Integer.valueOf(random.nextInt() / 10));
                record.setData("json string or other.");
                record.setTimestamp(new Long(System.currentTimeMillis() / 1000));
                ctx.collect(record);
            }
            
            Thread.sleep(200l);
        }
    }

    @Override
    public void cancel() {
        //用于结束source,比如定义一个变量,在run中进行判断,然后停止流
    }
}
