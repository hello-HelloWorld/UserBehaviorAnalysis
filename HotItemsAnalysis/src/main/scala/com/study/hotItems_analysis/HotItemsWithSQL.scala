package com.study.hotItems_analysis

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._

/*
* @author: helloWorld
* @date  : Created in 2020/8/26 15:56
*/

object HotItemsWithSQL {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    //设置时间语义为事件时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取文件
    val inputStream: DataStream[String] = environment.readTextFile("E:\\workspace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //创建表执行环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //将datastream注册成表
    tableEnvironment.createTemporaryView("userBehaviorTable", dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    //用sql实现topN
    val resultTable: Table = tableEnvironment.sqlQuery(
      """
        |select *
        | from (select *,row_number() over (partition by windowEnd order by cnt desc) as row_num from
        |         (select itemId,
        |            count(itemId) as cnt,
        |            hop_end(ts,interval '5' minute,interval '1' hour) as windowEnd
        |         from userBehaviorTable
        |         where behavior='pv'
        |         group by hop(ts,interval '5' minute,interval '1' hour),itemId)
        |     )
        |where row_num <=5
      """.stripMargin)

    resultTable.toRetractStream[(Long, Long, Timestamp, Long)].print()
    environment.execute("hotItems topN with sql")
  }
}
