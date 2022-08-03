package com.yili.apitest

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.{Properties, Random}


/**
 * @content 第五章 source api define
 * @author menglei
 * @created by 2022.08.02 11:04
 * @desc 温度传感器
 *
 */
object SourceTestFromCustomize {
  def main(args: Array[String]): Unit = {
    //1.创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //2.自定义source
    val stream = env.addSource(new MySensorSource())
    stream.print()
    env.execute()
  }
}

//自定义source function
class MySensorSource() extends SourceFunction[SensorReading] {

  var running = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //定义一个随机数生成器
    val random = new Random()
    //随机生成一组（10个）传感器的初始温度(id,temp)
    var curTemp = 1.to(10).map(i => ("sensor_" + i, random.nextDouble() * 100))
    while (running) {
      //在上次温度基础上更新温度值
      curTemp.map(
        data => (data._1, data._2 + random.nextGaussian())
      )
      //获取当前时间戳，加入到数据中,调用sourceContext.collect发出数据
      val curTime = System.currentTimeMillis()
      curTemp.foreach(data => sourceContext.collect(SensorReading(data._1, curTime, data._2)))
      //间隔500ms
      Thread.sleep(500)
    }
  }

  override def cancel(): Unit = running = false
}
