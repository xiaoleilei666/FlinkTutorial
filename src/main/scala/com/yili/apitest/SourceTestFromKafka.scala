package com.yili.apitest

import com.sun.org.apache.xerces.internal.utils.XMLSecurityPropertyManager.Property
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties


/**
 * @content 第五章 source api define
 * @author menglei
 * @created by 2022.08.02 11:04
 * @desc 温度传感器
 *
 */
object SourceTestFromKafka {
  def main(args: Array[String]): Unit = {
    //1.创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2.从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("group.id", "consumer-group")
    val kafkaStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    kafkaStream.print()
    env.execute()
  }
}
