package com.study.hotItems_analysis

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.table.api.scala._

/*
* @author: helloWorld
* @date  : Created in 2020/8/26 14:11
*/

object HotItemsWithTable {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    //设置时间语义为事件时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取文件数据
    val inputStream: DataStream[String] = environment.readTextFile("E:\\workspace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //调用table api，创建表执行环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //将datastream转换成表，提取需要的字段，进行处理
    val userBehaviorTable: Table = tableEnvironment.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    //分组开窗，增量聚合
    val aggTable: Table = userBehaviorTable
      .filter('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'tw)
      .groupBy('itemId, 'tw)
      .select('itemId, 'itemId.count as 'cnt, 'tw.end as 'windowEnd)

    //用sql实现分组取topN的功能
    tableEnvironment.createTemporaryView("aggTable", aggTable,'itemId,'cnt,'windowEnd)
    val resultTable: Table = tableEnvironment.sqlQuery(
      """
        |select *
        | from (select *,row_number() over (partition by windowEnd order by cnt desc) as row_num from aggTable)
        |where row_num <=5
      """.stripMargin)

    resultTable.toRetractStream[(Long, Long, Timestamp, Long)].print("result")
    environment.execute("hotItem topN with table api & sql")
  }
}
