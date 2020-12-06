package com.hug.realtime.app

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime}
import java.util

import com.alibaba.fastjson.JSON
import com.hug.mocker.constants.GmallConstant
import com.hug.realtime.beans.{CouponAlertInfo, EventLog}
import com.hug.realtime.utils.{MyEsUtil, MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
 * 同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且过程中没有浏览商品
 */
object AlertApp {
    
    private val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf,Seconds(5))
        
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstant.KAFKA_TOPIC_EVENT,ssc)
        
        //格式转换为样例类
        val eventLogDStream: DStream[EventLog] = kafkaDStream.map(record => {
            val value: String = record.value()
            val eventLog: EventLog = JSON.parseObject(value, classOf[EventLog])
            val dateAndHour: Array[String] = sdf.format(eventLog.ts).split(" ")
            eventLog.logDate = dateAndHour(0)
            eventLog.logHour = dateAndHour(1)
            eventLog
        })
        
        // 按照mid分组
        val midToLogIter: DStream[(String, EventLog)] = eventLogDStream.map(eventLog => (eventLog.mid, eventLog))
        
        // 开窗口
        val windowDStream: DStream[(String, Iterable[EventLog])] = midToLogIter.window(Minutes(5)).groupByKey()
        windowDStream.foreachRDD(rdd => {
            rdd.foreach(println)
        })
        // 窗口内部做数据分析
        val boolToAlertInfo: DStream[(Boolean, CouponAlertInfo)] = windowDStream.map { case (mid, iter) =>
            // 创建HashSet存放uid
            val uids = new util.HashSet[String]()
            // 存放优惠券涉及的商品id
            val itemIds = new util.HashSet[String]()
            // 存放发生过的行为
            val events = new util.ArrayList[String]()
        
            // 标记是否存在浏览商品行为
            var noClick: Boolean = true
        
            // 遍历iter
            breakable {
                iter.foreach(eventLog => {
                    // 添加事件信息
                    events.add(eventLog.evid)
                    // 统计领券的uid
                    if ("coupon".equals(eventLog.evid)) {
                        uids.add(eventLog.uid)
                        itemIds.add(eventLog.itemid)
                    } else if ("clickItem".equals(eventLog.evid)) {
                        noClick = false
                        break()
                    }
                })
            }
            //产生疑似预警信息
            (uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
        
        }
        boolToAlertInfo.foreachRDD(rdd => {
            println("*************\n")
            rdd.foreach(println)
        })
        // 产生预警日志
        val alertInfoDStream: DStream[CouponAlertInfo] = boolToAlertInfo.filter(_._1).map(_._2)
        
        alertInfoDStream.cache()
        alertInfoDStream.count().print()
        
        // 写入ES
        alertInfoDStream.foreachRDD(rdd => {
            rdd.foreachPartition(iter => {
                // 创建索引名 gmall_coupon_alert-2020-10-16
                val indexName = s"${PropertiesUtil.load("config.properties").getProperty("es.alert.prefix")}-${LocalDate.now()}"
                
                // 准备数据，添加docId
                val hourMinu: String = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm"))
                val docList: List[(String, CouponAlertInfo)] = iter.toList.map(alertInfo => (s"${alertInfo.mid}-$hourMinu", alertInfo))
                
                // 将数据写入ES
                MyEsUtil.insertBulk(indexName, docList)
            })
        })
        
        // 开启任务
        ssc.start()
        ssc.awaitTermination()
        
    }
}
