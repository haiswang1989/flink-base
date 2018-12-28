package com.haiswang.flink.demo.datatype;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Value;

/**
 * 实现了org.apache.flink.types.Value接口的类
 * 
 * 测试失败,抛出异常
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月15日 下午4:54:05
 */
public class DataTypeImplValueIntf {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<MyValueImpl> dsValue = env.fromElements(new MyValueImpl("wang"), new MyValueImpl("hai"));
        dsValue.print().setParallelism(1);
        
        try {
            env.execute("Data type class impl value interface");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

/**
 * 实现rg.apache.flink.types.Value的类
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月15日 下午4:56:44
 */
class MyValueImpl implements Value {
    
    private static final long serialVersionUID = 1L;
    
    private String str;
    
    public MyValueImpl(String strArgs) {
        this.str = strArgs;
    }
    
    @Override
    public void read(DataInputView in) throws IOException {
        str = in.readUTF();
    }
    
    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeUTF(str);
    }
    
    @Override
    public String toString() {
        return str;
    }
}
