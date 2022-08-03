package com.yili.apitest

import org.apache.flink.streaming.api.scala._

/**
 * @content 第五章 source api define
 * @author menglei
 * @created by 2022.08.02 11:04
 * @desc 温度传感器
 *
 */

//定义样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTestFromList {
  def main(args: Array[String]): Unit = {
    //1.创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2.从集合中读取数据
    val dataList = List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    )
    val stream = env.fromCollection(dataList)
    stream.print()
    env.execute()
  }
}
