package com.sdust.mall.publisher.service.impl;

import com.sdust.mall.publisher.mapper.TrademarkStatMapper;
import com.sdust.mall.publisher.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
@Service
public class MySQLServiceImpl implements MySQLService {
    @Autowired
    TrademarkStatMapper trademarkStatMapper;

    @Override
    public List<Map> getTrademarkStat(String startDate, String endDate, int topN) {
        return trademarkStatMapper.selectTradeSum(startDate,endDate,topN);
    }
}
