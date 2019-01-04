package com.haiswang.flink.demo.xiaoxiang.connector.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.haiswang.flink.demo.xiaoxiang.source.Record;

/**
 * 并行度为1的source
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月4日 上午10:14:49
 */
public class MySimpleSource implements SourceFunction<Record> {

    private static final long serialVersionUID = 1L;

    @Override
    public void run(SourceContext<Record> ctx)
            throws Exception {
    }

    @Override
    public void cancel() {
        
    }
}
