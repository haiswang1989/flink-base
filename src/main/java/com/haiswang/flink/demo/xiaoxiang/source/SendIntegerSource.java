package com.haiswang.flink.demo.xiaoxiang.source;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月2日 上午10:43:37
 */
public class SendIntegerSource implements SourceFunction<Integer> {

    private static final long serialVersionUID = 1L;

    @Override
    public void run(SourceContext<Integer> ctx)
            throws Exception {
        Random random = new Random();
        while(true) {
            int randInt = random.nextInt(100);
            ctx.collect(randInt);
            Thread.sleep(500l);
        }
    }

    @Override
    public void cancel() {

    }

}
