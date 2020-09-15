package com.study

import java.net.URL
import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/*
* @author: helloWorld
* @date  : Created in 2020/9/4 11:19
*/

/*
* 网站恶意登录监控
* 如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险，输出相关的信息进行报警提示
* */

// 定义输入输出的样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    //设置时间语义为事件时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val sourceUrl: URL = getClass.getResource("/LoginLog.csv")
    val path: String = sourceUrl.getPath
    val inputStream: DataStream[String] = environment.readTextFile(path)
    //将数据封装为样例类，并将eventTime做为watermarket的时间戳
    val dataStream: DataStream[LoginEvent] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      LoginEvent(dataArr(0).toLong, dataArr(1), dataArr(2), dataArr(3).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        //延迟3秒
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    // 用ProcessFunction进行转换，如果遇到2秒内连续2次登录失败，就输出报警
    val resutStream: DataStream[Warning] = dataStream
      .keyBy(_.userId)
      .process(new LoginFailWarning(2))

    dataStream.print("data")
    resutStream.print()
    environment.execute("login fail")
  }
}

// 实现自定义的ProcessFunction
class LoginFailWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  //定义List状态，用来保存2秒内所有登录失败事件
  lazy val loginEventState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("logain_fail", classOf[LoginEvent]))
  //定义value状态，用来保存定时器的时间戳
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    //判断当前数据是否登录失败
    if (value.eventType == "fail") {
      //如果是失败就添加到ListState中，如果没有注册定时器，就注册定时器
      loginEventState.add(value)
      if (timerTsState.value() == 0) {
        val ts: Long = value.eventTime * 1000L + 2000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
      }
    } else {
      //如果登录成功，删除定时器，重新开始
      ctx.timerService().deleteEventTimeTimer(timerTsState.value())
      loginEventState.clear()
      timerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    //如果2秒后的定时器触发了，那么判断ListState中失败的个数
    val allLoginFailList: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]
    val iter: util.Iterator[LoginEvent] = loginEventState.get().iterator()
    while (iter.hasNext) {
      allLoginFailList += iter.next()
    }

    if (allLoginFailList.length >= maxFailTimes) {
      out.collect(Warning(ctx.getCurrentKey, allLoginFailList.head.eventTime, allLoginFailList.last.eventTime, "login fail in 2s for " + allLoginFailList.length + " times."))
    }
    //清空状态
    loginEventState.clear()
    timerTsState.clear()
  }
}
