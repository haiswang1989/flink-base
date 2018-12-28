package com.haiswang.flink.demo.datatype;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.Date;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * JDK自带的Java对象
 * 
 * Flink支持绝大多数的Java的类,但是一些包含无法被序列化的对象的类被限制,
 * 比如IO对象,或者一些本地化的资源,JavaBean对象一般可以很好的工作对于一些不是Pojo类型的对象,
 * Flink处理的时候会当成一个黑盒,直接使用kvro进行序列化和反序列化,没法进行优化
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月15日 下午4:21:45
 */
public class DataTypeJavaClassType {

    public static void main(String[] args) {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Date> dsDate = env.fromElements(new Date(), new Date(), new Date());
        dsDate.print().setParallelism(1);
        
        //这些IO对象是可以的嘛,可能到了集群上就不行了,文件是本地的,如果传到其他的结点执行,文件就不存在了
        File file = new File("input.txt");
        FileInputStream fis_1 = null;
        FileInputStream fis_2 = null;
        FileInputStream fis_3 = null;
        try {
            fis_1 = new FileInputStream(file);
            fis_2 = new FileInputStream(file);
            fis_3 = new FileInputStream(file);
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        
        DataStream<FileInputStream> dsIoStream = env.fromElements(fis_1, fis_2, fis_3);
        dsIoStream.map(new MapFunction<FileInputStream, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String map(FileInputStream fis) throws Exception {
                StringBuilder resultBuilder = new StringBuilder();
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                String line = null;
                while(null!=(line=br.readLine())) {
                    resultBuilder.append(line).append(" ");
                }
                return resultBuilder.toString();
            }
        }).print().setParallelism(1);
        
        try {
            env.execute("Java Api Class Type");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
