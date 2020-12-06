package com.hug.realtime.app

import com.alibaba.fastjson.JSON
import com.hug.mocker.constants.GmallConstant
import com.hug.realtime.beans.OrderInfo
import com.hug.realtime.utils.{MyKafkaUtil, PropertiesUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object GmvApp {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GmvApp")
        // 创建StreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstant.KAFKA_TOPIC_ORDER_INFO, ssc)
        // 将每一行数据转换为样例类对象，并作数据脱敏
        val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {
            val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
            
            val dateAndTime: Array[String] = orderInfo.create_time.split(" ")
            orderInfo.create_date = dateAndTime(0)
            orderInfo.create_hour = dateAndTime(1).split(":")(0)
            
            //脱敏手机号
            val tuple: (String, String) = orderInfo.consignee_tel.splitAt(3)
            orderInfo.consignee_tel = tuple._1 + "****" + tuple._2.splitAt(4)._2
            println(orderInfo)
            orderInfo
        })
        
        orderInfoDStream.count().print()
        // 将数据保存到Phoenix
        orderInfoDStream.foreachRDD(rdd => {
            rdd.saveToPhoenix(PropertiesUtil.load("config.properties").getProperty("phoenix.order.table"),
                classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase),
                new Configuration,
                Some(PropertiesUtil.load("config.properties").getProperty("phoenix.zk.url"))
            )
        })
        //启动SparkStreamingContext
        ssc.start()
        ssc.awaitTermination()
        
    }
}
