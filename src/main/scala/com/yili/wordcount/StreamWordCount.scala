package com.yili.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author menglei
 * @desc 流处理 wordcount
 * @create by 2022.07.29
 * @version 1.0.0
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //从外部环境中获取参数
    val parm = ParameterTool.fromArgs(args)
    val hostname = parm.get("host")
    val port: Int = parm.getInt("port")

    //创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //接收socket文本流
    val textDstream = env.socketTextStream("localhost", 7777)
    //flatmap和map需要引用的隐式转换
    import org.apache.flink.api.scala._
    val resultDataStreamn = textDstream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty).disableChaining()
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    resultDataStreamn.print()
    //启动任务执行
    env.execute("start exec flink")
  }
}
