package com.study

import java.net.URL
import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/*
* @author: helloWorld
* @date  : Created in 2020/9/4 16:08
*/
/*
*使用CEP（Complex Event Processing，复杂事件处理）库，实现恶意登录
* */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    //创建流处理环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    //设置时间语义为事件时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取文件数据
    val url: URL = getClass.getResource("/LoginLog.csv")
    val path: String = url.getPath
    val inputStream: DataStream[String] = environment.readTextFile(path)

    //将数据封装样例类，设置时间戳和watermark
    val dataStream: DataStream[LoginEvent] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    //1.定义匹配模式
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("firstFail").where(_.eventType == "fail") //第一次登录失败
      .next("secondFail").where(_.eventType == "fail") //第二次失败
      .within(Time.seconds(2)) //在2秒之内检测匹配

    //2.在分组之后的数据流上应用模式，得到一个PatternStream
    val pattternStream: PatternStream[LoginEvent] = CEP.pattern(dataStream.keyBy(_.userId), loginFailPattern)

    //3.将检测到的事件序列，转换输出报警信息
    val resultStream: DataStream[Warning] = pattternStream.select(new LoginFailDetect())

    //4.打印输出
    resultStream.print()
    environment.execute("login fail with cep")
  }
}

// 自定义PatternSelectFunction，用来将检测到的连续登录失败事件，包装成报警信息输出
class LoginFailDetect() extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    // map里存放的就是匹配到的一组事件，key是定义好的事件模式名称
    val firstLoginFail: LoginEvent = map.get("firstFail").get(0)
    val secondLoginFail: LoginEvent = map.get("secondFail").get(0)

    Warning(firstLoginFail.userId, firstLoginFail.eventTime, secondLoginFail.eventTime, "login fail")
  }
}
