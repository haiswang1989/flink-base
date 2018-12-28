package com.haiswang.flink.demo.xiaoxiang.transformation.joinandcogroup;

public class Input2 {
    
    private String input2Uid;
    
    private String input2Attr;
    
    public String getInput2Uid() {
        return input2Uid;
    }

    public void setInput2Uid(String input2Uid) {
        this.input2Uid = input2Uid;
    }

    public String getInput2Attr() {
        return input2Attr;
    }

    public void setInput2Attr(String input2Attr) {
        this.input2Attr = input2Attr;
    }

    public Input2() {
        
    }
    
    public Input2(String input2UidArgs, String input2AttrArgs) {
        this.input2Uid = input2UidArgs;
        this.input2Attr = input2AttrArgs;
    }
    
    @Override
    public String toString() {
        return "input2 : " + input2Uid + " : " + input2Attr;
    }
}
