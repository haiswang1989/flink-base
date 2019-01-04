package com.haiswang.flink.demo.xiaoxiang.source;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月26日 下午5:40:08
 */
public class Record {
    
    private String bizName; //业务名称
    
    private int bizId; //业务方Id
    
    private long timestamp; //时间搓
    
    private int attr; //属性
    
    private String data; //原始属性描述
    
    private boolean hasWatermarkMarker = false;

    public String getBizName() {
        return bizName;
    }

    public void setBizName(String bizName) {
        this.bizName = bizName;
    }

    public int getBizId() {
        return bizId;
    }

    public void setBizId(int bizId) {
        this.bizId = bizId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getAttr() {
        return attr;
    }

    public void setAttr(int attr) {
        this.attr = attr;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
    
    public boolean hasWatermarkMarker() {
        return hasWatermarkMarker;
    }
}
