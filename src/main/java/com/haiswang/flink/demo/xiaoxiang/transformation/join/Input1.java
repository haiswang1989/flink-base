package com.haiswang.flink.demo.xiaoxiang.transformation.join;

public class Input1 {
    
    private String input1Uid;
    
    private String input1Attr;
    
    public String getInput1Uid() {
        return input1Uid;
    }

    public void setInput1Uid(String input1Uid) {
        this.input1Uid = input1Uid;
    }

    public String getInput1Attr() {
        return input1Attr;
    }

    public void setInput1Attr(String input1Attr) {
        this.input1Attr = input1Attr;
    }

    public Input1() {
        
    }
    
    public Input1(String input1UidArgs, String input1AttrArgs) {
        this.input1Uid = input1UidArgs;
        this.input1Attr = input1AttrArgs;
    }
    
    @Override
    public String toString() {
        return "input1 : " + input1Uid + " : " + input1Attr;
    }
}
