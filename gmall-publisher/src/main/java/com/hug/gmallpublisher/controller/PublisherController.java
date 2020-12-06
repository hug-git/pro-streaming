package com.hug.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.hug.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date) {

        //1.查询日活总数
        Integer dauTotal = publisherService.getDauTotal(date);

        //2.创建集合用于存放最终的数据
        ArrayList<Map> result = new ArrayList<>();

        //3.创建日活Map并赋值
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //4.创建新增设备Map并赋值
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);

        //创建交易额Map并赋值
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "gmv");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", publisherService.getOrderAmount(date));

        //将map放入集合
        result.add(dauMap);
        result.add(newMidMap);
        result.add(gmvMap);

        //返回结果
        return JSON.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getRealTimeHours(@RequestParam("id") String id, @RequestParam("date") String date) {

        // 创建Map用于存放最终结果
        HashMap<String, Map> result = new HashMap<>();
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        Map todayMap = null;
        Map yesterdayMap = null;
        if ("dau".equals(id) || "new_mid".equals(id)) {
            // 查询今天的分时数据
            todayMap = publisherService.getDauHourTotal(date);
            // 查询昨天的分时数据
            yesterdayMap = publisherService.getDauHourTotal(yesterday);
        } else if ("gmv".equals(id)) {
            // 查询今天的分时数据
            todayMap = publisherService.getOrderAmountHour(date);
            // 查询昨天的分时数据
            yesterdayMap = publisherService.getOrderAmountHour(yesterday);
        }


        // 将今天以及昨天的分时数据放入result
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        // 转换为JSON数据做返回
        return JSON.toJSONString(result);
    }

    @RequestMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,
                                @RequestParam("startpage") int startpage,
                                @RequestParam("size") int size,
                                @RequestParam("keyword") String keyword) {
        String result = "";
        try {
            result = publisherService.getSaleDetail(date, startpage, size, keyword);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
