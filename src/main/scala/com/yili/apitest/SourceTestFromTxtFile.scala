package com.yili.apitest

import org.apache.flink.streaming.api.scala._

/**
 * @content 第五章 source api define
 * @author menglei
 * @created by 2022.08.02 11:04
 * @desc 温度传感器
 *
 */
object SourceTestFromTxtFile {
  def main(args: Array[String]): Unit = {
    //1.创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2.从文本文件中读取数据
    val datapath = "E:\\study\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensorreading"
    val txtDataStream = env.readTextFile(datapath)
    txtDataStream.print()
    env.execute()
  }
}
