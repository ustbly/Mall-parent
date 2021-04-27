package com.sdust.mall.publisher.service;

import java.util.Map;

/**
 * 操作ES的接口
 */
public interface ESService {
    //查询某天的日活数
    public Long getDauTotal(String date);

    //查询某天某时段的日活数
    public Map<String,Long> getDauHour(String date);
}
