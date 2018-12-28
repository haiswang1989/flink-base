package com.haiswang.flink.demo.xiaoxiang.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月27日 上午9:52:14
 */
public class SocketSource implements SourceFunction<String> {
    
    private static final long serialVersionUID = 1L;

    @Override
    public void run(SourceContext<String> ctx)
            throws Exception {
        
    }

    @Override
    public void cancel() {
    }
    
}
