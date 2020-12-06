package com.hug.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hug.mocker.constants.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
public class LoggerController {
    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping("test")
    public String testMe(@RequestParam(value = "str",defaultValue = "default") String str){
        return str + " success";
    }
    @PostMapping("log")
    public String doLog(@RequestParam("logString") String logString ){


        // 0 补充时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());
        // 1 落盘 file
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);


        // 2 推送到kafka
        if( "startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_START,jsonString);
        }else{
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonString);
        }

        return "success";
    }

}
