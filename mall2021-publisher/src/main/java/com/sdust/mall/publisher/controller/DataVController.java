package com.sdust.mall.publisher.controller;

import com.sdust.mall.publisher.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 对接 DataV 的 Controller
 */
@RestController
public class DataVController {
    @Autowired
    MySQLService mysqlService;

    @RequestMapping("/trademark-sum")
    public Object trademarkSum(@RequestParam("start_date") String startDate,
                               @RequestParam("end_date") String endDate,
                               @RequestParam("topN") int topN) {
        //Map:{"trademark_id":"001","trademark_name":"小米","amount":1000}
        //{"x":"品牌名称","y":当前品牌交易额汇总,"s":1}
        List<Map> mapList = mysqlService.getTrademarkStat(startDate, endDate, topN);
        List<Map> rsList = new ArrayList<>();
        for (Map map : mapList) {
            Map rsMap = new HashMap();
            rsMap.put("x",map.get("trademark_name"));
            rsMap.put("y",map.get("amount"));
            rsMap.put("s",1);
            rsList.add(rsMap);
        }

        return rsList;
    }
}
