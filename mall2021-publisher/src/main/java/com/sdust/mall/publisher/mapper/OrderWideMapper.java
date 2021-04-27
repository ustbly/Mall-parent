package com.sdust.mall.publisher.mapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * 订单宽表进行操作的接口
 */
public interface OrderWideMapper {
    //获取指定日期的交易额
    public BigDecimal selectOrderAmountTotal(String date);
    //获取指定日期的分时交易额
    public List<Map> selectOrderAmountHour(String date);
}
