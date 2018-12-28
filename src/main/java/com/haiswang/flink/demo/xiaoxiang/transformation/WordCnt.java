package com.haiswang.flink.demo.xiaoxiang.transformation;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月27日 下午2:53:39
 */
public class WordCnt {
    
    private String word;
    
    private Integer cnt;
    
    public WordCnt() {
        
    }
    
    public WordCnt(String wordArgs, Integer cntArgs) {
        this.word = wordArgs;
        this.cnt = cntArgs;
    }
    
    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCnt() {
        return cnt;
    }

    public void setCnt(Integer cnt) {
        this.cnt = cnt;
    }
    
    @Override
    public String toString() {
        return word + " : " + cnt;
    }
}
