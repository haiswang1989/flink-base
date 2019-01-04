package com.haiswang.flink.demo.xiaoxiang.source;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月28日 下午5:21:08
 */
public class SendSentenceSource implements SourceFunction<String> {
    
    private static final long serialVersionUID = 1L;

    @Override
    public void run(SourceContext<String> ctx)
            throws Exception {
        String[] sentences = {"i am wang hai sheng" ,"i am wu xiao tian", "i am wang hai ming", "i am liu xiao lan"}; 
        Random random = new Random();
        while(true) {
            int randomInt = random.nextInt(100);
            ctx.collect(sentences[randomInt%2]);
            Thread.sleep(2000l);
        }
    }

    @Override
    public void cancel() {
    }
}
