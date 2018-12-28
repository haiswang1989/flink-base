package com.haiswang.flink.demo.xiaoxiang.transformation.joinandcogroup;

public class Input1JoinInput2 {
    
    private String uid;
    private String input1Attr;
    private String input2Attr;
    
    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getInput1Attr() {
        return input1Attr;
    }

    public void setInput1Attr(String input1Attr) {
        this.input1Attr = input1Attr;
    }

    public String getInput2Attr() {
        return input2Attr;
    }

    public void setInput2Attr(String input2Attr) {
        this.input2Attr = input2Attr;
    }

    public Input1JoinInput2() {
    }
    
    public Input1JoinInput2(Input1 input1, Input2 input2) {
        this.uid = input1.getInput1Uid();
        this.input1Attr = input1.getInput1Attr();
        this.input2Attr = input2.getInput2Attr();
    }
    
    @Override
    public String toString() {
        return uid + " : " + input1Attr + " : " + input2Attr;
    }
}
