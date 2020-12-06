package com.hug.realtime.app

import com.alibaba.fastjson.JSON
import com.hug.mocker.constants.GmallConstant
import com.hug.realtime.beans.UserInfo
import com.hug.realtime.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object SaveUserToRedis {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("SaveUserToRedis$").setMaster("local[*]")
        
        // 初始化SparkSteamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstant.KAFKA_TOPIC_USER_INFO,ssc)
        
        // 提取value
        val userInfoStrDStream: DStream[String] = kafkaDStream.map(_.value())
        
        // 写入Redis
        userInfoStrDStream.foreachRDD(rdd => {
            rdd.cache()
            rdd.foreachPartition(iter => {
                val jedisClient: Jedis = RedisUtil.getJedisClient
                iter.foreach(userInfoStr => {
                    val userInfo: UserInfo = JSON.parseObject(userInfoStr,classOf[UserInfo])
                    val redisKey = s"UserInfo:${userInfo.id}"
                    jedisClient.set(redisKey,userInfoStr)
                })
                jedisClient.close()
            })
        })
        
        userInfoStrDStream.count().print()
        
        //启动SparkStreamingContext
        ssc.start()
        ssc.awaitTermination()
    }
}
