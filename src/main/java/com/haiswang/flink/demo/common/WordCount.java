package com.haiswang.flink.demo.common;

/**
 * 
 * 作为keyBy入参的对象,必须是Pojo对象
 * 
 * 类的成员必须是Public或者非Public提供Get和Set方法
 * 必须提供无参构造函数
 * 
 * 类必须是Public的
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月13日 下午4:20:22
 */
public final class WordCount {

    private String word;
    private int count;
    
    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public WordCount() {
    }

    public WordCount(String wordArgs, int countArgs) {
        this.word = wordArgs;
        this.count = countArgs;
    }
    
    @Override
    public String toString() {
        return word + ":" + count;
    }
}
