package com.yili.wordcount

import org.apache.flink.api.scala._

/**
 * @author menglei
 * @desc 批处理 wordcount
 * @create by 2022.07.29
 * @version 1.0.0
 */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //从文件中读取数据
    val inputPath = "E:\\study\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDS = env.readTextFile(inputPath)
    val wordCountDS = inputDS.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    wordCountDS.print()
  }
}
