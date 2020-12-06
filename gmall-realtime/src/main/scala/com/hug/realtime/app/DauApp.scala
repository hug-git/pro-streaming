package com.hug.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.hug.mocker.constants.GmallConstant
import com.hug.realtime.beans.StartUpLog
import com.hug.realtime.handler.DauHandler
import com.hug.realtime.utils.{MyKafkaUtil, PropertiesUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {
    private val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("Spark03_WordCount_Kafka$").setMaster("local[*]")
    
        // 初始化SparkSteamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))
    
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstant.KAFKA_TOPIC_START,ssc)
        
        // 将数据转换为样例类对象
        val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {
            // 获取数据
            val value: String = record.value()
            // 转换为样例类
            val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
            // 取出时间戳，并格式化
            val dateHourStr: String = sdf.format(new Date(startUpLog.ts))
            val dateAndHour: Array[String] = dateHourStr.split(" ")
            startUpLog.logDate = dateAndHour(0)
            startUpLog.logHour = dateAndHour(1)
            startUpLog
        })
        
        startUpLogDStream.cache()
        startUpLogDStream.count().print()
        
        //跨批次去重
        val filterByRedisLogDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)
        filterByRedisLogDStream.cache()
        filterByRedisLogDStream.count().print()
        
        // 同批次去重
        val filterByMidLogDStream: DStream[StartUpLog] = DauHandler.filterByMid(filterByRedisLogDStream)
        filterByMidLogDStream.count().print()
        
        // 将Mid写入Redis
        DauHandler.saveMidToRedis(startUpLogDStream)
        
        // 数据明细写入Phoenix
        filterByMidLogDStream.foreachRDD(rdd => {
            rdd.saveToPhoenix(PropertiesUtil.load("config.properties").getProperty("phoenix.dau.table"),
                //Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
                classOf[StartUpLog].getDeclaredFields.map(_.getName.toUpperCase),
                new Configuration,
                Some(PropertiesUtil.load("config.properties").getProperty("phoenix.zk.url"))
            )
        })
        
        //启动SparkStreamingContext
        ssc.start()
        ssc.awaitTermination()
    }
    
}
