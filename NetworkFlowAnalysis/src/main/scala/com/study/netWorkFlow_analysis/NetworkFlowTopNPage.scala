package com.study.netWorkFlow_analysis

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/*
* @author: helloWorld
* @date  : Created in 2020/8/26 16:55
*/
/**
  * 实现“热门页面浏览数”的统计，也就是读取服务器日志中的每一行log，统计在一段时间内用户访问每一个url的次数，然后排序输出显示。
  * 具体做法为：每隔5秒，输出最近10分钟内访问量最多的前N个URL。
  */

/*
  * @Description: 定义输入样例类
  * @param ip        : 访问的 IP
  * @param userId    : 访问的 user ID
  * @param eventTime : 访问时间
  * @param method    : 访问方法 GET/POST/PUT/DELETE
  * @param url       : 访问的 url
  **/
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

/**
  * @Description: 定义聚合结果样例类
  * @param url       :访问的url
  * @param windowEnd :窗口结束时间
  * @param count     :时间窗口内url的访问总数
  **/
case class PageViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlowTopNPage {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    //设置时间语义为事件时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val inputStream: DataStream[String] = environment.readTextFile("E:\\workspace\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
    val dataStream: DataStream[ApacheLogEvent] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(" ")
      //将时间字段转换为时间戳
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yy:HH:mm:ss")
      val timestamp: Long = dateFormat.parse(dataArr(3)).getTime
      ApacheLogEvent(dataArr(0), dataArr(1), timestamp, dataArr(5), dataArr(6))
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        //设置哪个字段为时间戳
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime * 1000L
      })

    //开窗聚合
    val outPutTag: OutputTag[ApacheLogEvent] = new OutputTag[ApacheLogEvent]("lata data")
    dataStream
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(outPutTag)
      .aggregate(new PageCountAgg(), new PageCountWindow())

  }

}
