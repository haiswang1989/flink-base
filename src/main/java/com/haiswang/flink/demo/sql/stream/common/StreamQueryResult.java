package com.haiswang.flink.demo.sql.stream.common;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月15日 下午4:13:33
 */
public class StreamQueryResult {
    
    private Long id;
    
    private String name;
    
    private Integer age;
    
    public StreamQueryResult() {}

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
    
}
