package com.hug.realtime.handler

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.hug.realtime.beans.StartUpLog
import com.hug.realtime.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
    
    /**
     * 同批次去重
     * @param filterByRedisLogDStream 根据Redis去重之后的数据
     * @return
     */
    def filterByMid(filterByRedisLogDStream: DStream[StartUpLog]): DStream[StartUpLog] = {
        val midDateToLogDStream: DStream[(String, StartUpLog)] = filterByRedisLogDStream.map(startUpLog => {
            (s"${startUpLog.mid}-${startUpLog.logDate}", startUpLog)
        })
        val midDateToLogDStreamIter: DStream[(String, Iterable[StartUpLog])] = midDateToLogDStream.groupByKey()
        
        midDateToLogDStreamIter.flatMap{case (_, iter) =>
            iter.toList.sortWith(_.ts < _.ts).take(1)
        }
    }
    
    private val sdf = new SimpleDateFormat("yyyy-MM-dd")
    
    /**
     * 跨批次去重
     * @param startUpLogDStream 从Kafka消费到的原始数据
     * @param sc sparkContext
     * @return
     */
    def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext): DStream[StartUpLog] = {
        // 方案一: 每条数据都获取和释放连接，效率低
        val value1: DStream[StartUpLog] = startUpLogDStream.filter(startUpLog => {
            val jedisClient: Jedis = RedisUtil.getJedisClient
            val exist: lang.Boolean = jedisClient.sismember(s"DAU:${startUpLog.logDate}", startUpLog.mid)
            jedisClient.close()
            !exist
        })
        value1
        
        // 方案二: 采用一个分区获取一次连接
        val value2: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
            rdd.mapPartitions(iter => {
                val jedisClient: Jedis = RedisUtil.getJedisClient
                val logs: Iterator[StartUpLog] = iter.filter(startUpLog => {
                    val exist: lang.Boolean = jedisClient.sismember(s"DAU:${startUpLog.logDate}", startUpLog.mid)
                    !exist
                })
                jedisClient.close()
                logs
            })
        })
        value2
        
        // 方案三: 使用广播变量将前置批次Redis中所有数据进行广播
        val value3: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
            val jedisClient: Jedis = RedisUtil.getJedisClient
            // 查询Redis
            val midSet: util.Set[String] = jedisClient.smembers(s"DAU:${sdf.format(new Date(System.currentTimeMillis()))}")
            // 广播
            val midSetBC: Broadcast[util.Set[String]] = sc.broadcast(midSet)
            // 关闭jedis
            jedisClient.close()
            // 在Executor进行过滤操作
            rdd.filter(startUpLog => !midSetBC.value.contains(startUpLog.mid))
        })
        value3
        
        
    }
    
    /**
     *  将去重之后的数据中的Mid写入Redis(给当天以后的批次去重使用)
     * @param startUpLogDStream 经过两次去重之后的数据集
     */
    def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]): Unit = {
        startUpLogDStream.foreachRDD(rdd => {
            //使用分区操作代替单跳数据操作
            rdd.foreachPartition(iter => {
                val jedisClient: Jedis = RedisUtil.getJedisClient
                
                iter.foreach(startUpLog => {
                    val redisKey = s"DAU:${startUpLog.logDate}"
                    jedisClient.sadd(redisKey,startUpLog.mid)
                })
                
                jedisClient.close()
            })
        })
    }
    
}
