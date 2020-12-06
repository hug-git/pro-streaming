package com.hug.realtime.utils

import java.lang
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object MyKafkaUtil {
    // 1.创建配置信息对象
    private val properties: Properties = PropertiesUtil.load("config.properties")
    
    // 2.初始化链接集群地址
    private val broker_list: String = properties.getProperty("kafka.broker.list")
    private val groupId: String = properties.getProperty("kafka.group.id")
    
    // 3.kafka消费者配置
    private val kafkaParam = Map(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.GROUP_ID_CONFIG -> groupId,
        //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
        //可以使用这个配置，latest自动重置偏移量为最新的偏移量
        "auto.offset.reset" -> "latest",
        //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
        //如果是false，会需要手动维护kafka偏移量
        "enable.auto.commit" -> (true: lang.Boolean)
    )
    
    
    // 创建DStream，返回接收到的输入数据
    def getKafkaDStream(topic: String,ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] ={
        val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaParam))
        dStream
        
    }
    
}
