package com.hug.realtime.app

import java.sql.Connection
import java.time.LocalDate
import java.util

import com.alibaba.fastjson.JSON
import com.hug.mocker.constants.GmallConstant
import com.hug.realtime.beans.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.hug.realtime.utils.{JdbcUtil, MyEsUtil, MyKafkaUtil, PropertiesUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object SaleDetailApp {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")
        // 创建StreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        
        // 读取kafka中orderInfo和OrderDetail数据,并转换为 K_V 结构
        val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstant.KAFKA_TOPIC_ORDER_INFO, ssc)
        val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL, ssc)
        
        // 将两个流转换为样例类对象
        val orderIdToInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.map(record => {
            val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
            
            val dateAndTime: Array[String] = orderInfo.create_time.split(" ")
            orderInfo.create_date = dateAndTime(0)
            orderInfo.create_hour = dateAndTime(1).split(":")(0)
            
            //脱敏手机号
            val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
            orderInfo.consignee_tel = tuple._1 + "********"
            
            (orderInfo.id, orderInfo)
        })
        
        val orderIdToDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map(record => {
            val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
            (orderDetail.order_id, orderDetail)
        })
        
        // fullJoin加Redis缓存
        val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderIdToInfoDStream.fullOuterJoin(orderIdToDetailDStream)
        val noUserSaleDetailDStream: DStream[SaleDetail] = fullJoinDStream.mapPartitions(iter => {
            // 存放detail的前置数据
            val details = new ListBuffer[SaleDetail]
            // 获取Redis连接
            val jedisClient: Jedis = RedisUtil.getJedisClient
            implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
        
            // 遍历fullJoin后的数据
            iter.foreach { case (orderId, (infoOpt, detailOpt)) =>
                val orderInfoKey: String = s"OrderInfo:$orderId"
                val orderDetailKey: String = s"orderDetail:$orderId"
    
    
                if (infoOpt.isDefined) { // =============infoOpt不为空===================
                    // 获取info数据
                    val orderInfo: OrderInfo = infoOpt.get
                    // ---------detailOpt不为空，则生成SaleDetail，写入Redis--------
                    if (detailOpt.isDefined) {
                        val orderDetail: OrderDetail = detailOpt.get
                        val saleDetail = new SaleDetail(orderInfo, orderDetail)
                        details += saleDetail
                    }
                
                    // 写入Redis
                    val orderInfoJson: String = Serialization.write(orderInfo)
                    jedisClient.set(orderInfoKey, orderInfoJson)
                    jedisClient.expire(orderInfoKey, 100)
                
                    // 获取Detail前置批次进行匹配
                    val orderDetailSet: util.Set[String] = jedisClient.smembers(orderDetailKey)
                    for (orderDetailJson <- orderDetailSet.asScala) {
                        val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                        val saleDetail = new SaleDetail(orderInfo, orderDetail)
                        details += saleDetail
                    }
                } else { // ===========infoOpt为空,detailInfo写入Redis缓存==================
                    // 获取Detail数据
                    val orderDetail: OrderDetail = detailOpt.get
                    // 与info前置数据匹配
                    if (jedisClient.exists(orderInfoKey)) { // 匹配到数据
                        val orderInfoJson: String = jedisClient.get(orderInfoKey)
                        val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
                        details += new SaleDetail(orderInfo, orderDetail)
//                        val orderDetailJson: String = Serialization.write(orderDetail)
//                        jedisClient.srem(orderDetailKey, orderDetailJson)
                    } else { // 未匹配到数据,写入Redis缓存
                        val orderDetailJson: String = Serialization.write(orderDetail)
                        jedisClient.sadd(orderDetailKey, orderDetailJson)
                    }
                }
            }
            // 释放Redis连接
            jedisClient.close()
            // 最终返回值
            details.toIterator
        })
        
        // 打印测试
//        noUserSaleDetailDStream.print()
        
        // 根据userId获取Redis中用户信息,补全信息
        val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(iter => {
            val jedisClient: Jedis = RedisUtil.getJedisClient
            val details: Iterator[SaleDetail] = iter.map(noUserSaleDetail => {
                val userRedisKey = s"UserInfo:${noUserSaleDetail.user_id}"
                if (jedisClient.exists(userRedisKey)) {
                    val userInfoStr: String = jedisClient.get(userRedisKey)
                    val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
                    noUserSaleDetail.mergeUserInfo(userInfo)
                } else { // ==========from MySql
                    val connection: Connection = JdbcUtil.getConnection
                    val userInfoJson: String = JdbcUtil.getUserInfoFromMysql(connection,
                        "select * from gmall2020.user_info where id =?",
                        Array(noUserSaleDetail.user_id))
                    val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
                    noUserSaleDetail.mergeUserInfo(userInfo)
                    connection.close()
                }
                noUserSaleDetail
            })
            jedisClient.close()
            details
        })
        saleDetailDStream.count().print()
//        saleDetailDStream.foreachRDD(rdd => {
//            rdd.foreach(println)
//        })
        // 写入ES
        saleDetailDStream.foreachRDD(rdd => {
            //创建索引名 gmall0523_sale_detail-2020-10-19
            rdd.foreachPartition(iter => {
                val indexName = s"${PropertiesUtil.load("config.properties").getProperty("es.sale.prefix")}-${LocalDate.now()}"
                val docList: List[(String, SaleDetail)] = iter.toList.map(saleDetail => (s"${saleDetail.order_id}-${saleDetail.order_detail_id}",saleDetail))
                MyEsUtil.insertBulk(indexName,docList)
            })
        })
        
        
        // 启动任务
        ssc.start()
        ssc.awaitTermination()
    }
}
