package com.sdust.mall.publisher.service;

import java.util.List;
import java.util.Map;

public interface MySQLService {
    public List<Map> getTrademarkStat(String startDate, String endDate, int topN);
}
