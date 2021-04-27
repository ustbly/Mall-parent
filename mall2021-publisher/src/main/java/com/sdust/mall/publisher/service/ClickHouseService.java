package com.sdust.mall.publisher.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * ClickHouse相关的业务接口
 */
public interface ClickHouseService {
    //获取指定日期总交易额
    BigDecimal getOrderAmountTotal(String date);
    //获取指定日期分时交易额

    /**
     * 从Mapper获取的分时交易额的格式：
     * List<Map{hr->11,am->10000}> ==> Map{11->10000,12->20000}
     * @param date
     * @return
     */
    Map<String,BigDecimal> getOrderAmountHour(String date);
}
