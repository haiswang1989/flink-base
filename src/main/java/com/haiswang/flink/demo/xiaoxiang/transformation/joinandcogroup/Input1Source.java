package com.haiswang.flink.demo.xiaoxiang.transformation.joinandcogroup;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Input1Source implements SourceFunction<Input1> {
    
    private static final long serialVersionUID = 1L;
    
    private Random random = new Random();
    
    @Override
    public void run(SourceContext<Input1> ctx)
            throws Exception {
        while(true) {
            int randInt = random.nextInt();
            ctx.collect(new Input1((randInt % 5) + "", randInt + ""));
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        
    }
}
