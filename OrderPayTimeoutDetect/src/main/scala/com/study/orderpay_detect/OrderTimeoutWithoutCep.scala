package com.study.orderpay_detect

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/*
* @author: helloWorld
* @date  : Created in 2020/9/7 14:20
*/


object OrderTimeoutWithoutCep {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    //设置时间语义为事件时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据文件
    val url: URL = getClass().getResource("/OrderLog.csv")
    val path: String = url.getPath
    val inputStream: DataStream[String] = environment.readTextFile(path)
    //封装成样例类
    val dataStream: DataStream[OrderEvent] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
      override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
    })

    // 自定义Process Function， 做精细化的流程控制
    val resultStream: DataStream[OrderResult] = dataStream
      .keyBy(_.orderId)
      .process(new OrderPayMatchDetect())

    resultStream.print()
    resultStream.getSideOutput(new OutputTag[OrderResult]("timeout")).print("timeout")
    environment.execute("order job")
  }
}

// 实现自定义KeyedProcessFunction，主流输出正常支付的订单，侧输出流输出超时报警订单
class OrderPayMatchDetect() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
  // 定义状态，用来保存是否来过create和pay事件的标识位，以及定时器时间戳
  lazy val isPayStated: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is payed", classOf[Boolean]))
  lazy val isCreataStated: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is created", classOf[Boolean]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  val timeoutOutPutTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("timeout")

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {

    //先取当前状态
    val isPayed: Boolean = isPayStated.value()
    val isCreated: Boolean = isCreataStated.value()
    val timerTs: Long = timerTsState.value()

    // 判断当前事件的类型，分成不同情况讨论
    // 情况1： 来的是create，要继续判断之前是否有pay来过
    if (value.eventType == "create") {
      // 情况1.1： 如果已经pay过的话，匹配成功，输出到主流，清空状态,删除定时器
      if (isPayed) {
        out.collect(OrderResult(value.orderId, "payed successfully"))
        isPayStated.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      } else {
        // 情况1.2：如果没pay过，那么就注册一个15分钟后的定时器，更新状态，开始等待
        val ts = value.eventTime * 1000L + 15 * 60 * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
        isCreataStated.update(true)
      }
    }
    // 情况2： 来的是pay，要继续判断是否来过create
    else if (value.eventType == "pay") {
      // 情况2.1：如果create已经来过，匹配成功，要继续判断间隔时间是否超过了15分钟
      if (isCreated) {
        // 情况2.1.1： 如果没有超时，正常输出结果到主流
        if (value.eventTime * 1000L < timerTs) {
          out.collect(OrderResult(value.orderId, "pay successfully"))
        }
        // 情况2.1.2： 如果已经超时，输出timeout报警到侧输出流
        else {
          ctx.output(timeoutOutPutTag, OrderResult(value.orderId, "payed but already timeout"))
        }
        // 不论哪种情况，都已经有了输出，清空状态
        isCreataStated.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      }
      // 情况2.2：如果create没来，需要等待乱序create，注册一个当前pay时间戳的定时器
      else {
        val ts: Long = value.eventTime * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
        isPayStated.update(true)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    //定时器触发，判断是哪种情况
    if (isPayStated.value()) {
      // 如果pay过，那么说明create没来，可能出现数据丢失异常情况
      ctx.output(timeoutOutPutTag, OrderResult(ctx.getCurrentKey, "already payed but not found created log"))
    } else {
      // 如果没有pay过，那么说明真正15分钟超时
      ctx.output(timeoutOutPutTag, OrderResult(ctx.getCurrentKey, "order timeOut"))
    }
    isPayStated.clear()
    isCreataStated.clear()
    timerTsState.clear()
  }
}

