package com.haiswang.flink.demo.sql.batch;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月15日 上午10:29:35
 */
public class GroupByCountResult {
    
    private String hd_svip_handle;
    
    private Long resultCount;
    
    public String getHd_svip_handle() {
        return hd_svip_handle;
    }

    public void setHd_svip_handle(String hd_svip_handle) {
        this.hd_svip_handle = hd_svip_handle;
    }

    public Long getResultCount() {
        return resultCount;
    }

    public void setResultCount(Long resultCount) {
        this.resultCount = resultCount;
    }

    public GroupByCountResult() {}
}
