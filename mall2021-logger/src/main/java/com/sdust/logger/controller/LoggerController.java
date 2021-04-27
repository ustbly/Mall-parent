package com.sdust.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


/**
 * @author LinYue
 * @email ustb2021@126.com
 * @create 2021-03-18 19:48
 * @desc 接受模拟器生成的数据，并对数据进行处理
 */

//@Controller //将对象的创建交给Spring容器
//方法返回String，默认会当作跳转页面处理

//@RestController = @Controller + @ResponseBody
//方法返回Object(包括String)，会转换为json格式字符串进行响应
@RestController
@Slf4j
public class LoggerController {

    //Spring提供的对kafka的支持
    @Autowired      //注入    将KafkaTemplate注入到Controller中
    KafkaTemplate kafkaTemplate;

    //http://localhost:8080/applog
    //提供一个方法，处理模拟器生成的数据
    //@RequestMapping("/applog")    把applog请求，交给applog方法进行处理
    //@RequestBody  表示从请求体中获取数据
    @RequestMapping("/applog")

    public String applog(@RequestBody String mockLog) {
        //输出日志
        //System.out.println(mockLog);

        //落盘
        log.info(mockLog);

        //根据日志类型，发送到kafka不同的主题去
        //将接收到的字符串数据转换为json对象
        JSONObject jsonObject = JSON.parseObject(mockLog);
        JSONObject startJson = jsonObject.getJSONObject("start");

        if (startJson != null) {
            //启动日志
            kafkaTemplate.send("mall_start_0319",mockLog);
        } else {
            //事件日志
            kafkaTemplate.send("mall_event_0319",mockLog);
        }
        return "success";
    }
}
