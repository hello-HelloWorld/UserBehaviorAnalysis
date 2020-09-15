package com.study.orderpay_detect

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/*
* @author: helloWorld
* @date  : Created in 2020/9/8 15:45
*/
/*
* 来自两条流的订单交易匹配
* 对于订单支付事件，用户支付完成其实并不算完，我们还得确认平台账户上是否到账了。
* 而往往这会来自不同的日志信息，所以我们要同时读入两条流的数据来做合并处理。
* 这里我们利用connect将两条流进行连接，然后用自定义的CoProcessFunction进行处理。
* */

// 定义到账数据的样例类
case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)

object OrderPayTxMatch {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取文件数据转化成样例类
    val url: URL = getClass.getResource("/OrderLog.csv")
    val orderEventStream: KeyedStream[OrderEvent, String] = environment.readTextFile(url.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })
      .filter(_.txId != "")
      .keyBy(_.txId)

    val url2: URL = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream: KeyedStream[ReceiptEvent, String] = environment.readTextFile(url2.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(3)) {
        override def extractTimestamp(element: ReceiptEvent): Long = element.timestamp * 1000L
      })
      .keyBy(_.txId)

    // 用connect连接两条流，匹配事件进行处理
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.connect(receiptEventStream).process(new OrderPayTxDetect())

    val unmatchedPays = new OutputTag[OrderEvent]("unmatched-pays")
    val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatched-receipts")

    resultStream.print("matched")
    resultStream.getSideOutput(unmatchedPays).print("unmatched-pays")
    resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts")
    environment.execute("order pay tx match job")
  }
}

// 自定义CoProcessFunction，实现两条流数据的匹配检验
class OrderPayTxDetect() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  // 用两个ValueState，保存当前交易对应的支付事件和到账事件
  lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))
  lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))

  val unMatchedPays: OutputTag[OrderEvent] = new OutputTag[OrderEvent]("unmatched-pays")
  val unMatchedReceipts: OutputTag[ReceiptEvent] = new OutputTag[ReceiptEvent]("unmatched-receipts")

  override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // pay来了，考察有没有对应的receipt来过
    val receipt: ReceiptEvent = receiptState.value()
    if (receipt != null) {
      // 如果已经有receipt，那么正常匹配，输出到主流
      out.collect(value, receipt)
      receiptState.clear()
    } else {
      // 如果receipt还没来，那么把pay存入状态，注册一个定时器等待5秒
      payState.update(value)
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L)
    }
  }

  override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // receipt来了，考察有没有对应的pay来过
    val pay: OrderEvent = payState.value()
    if (pay != null) {
      // 如果已经有pay，那么正常匹配，输出到主流
      out.collect(pay, value)
      payState.clear()
    } else {
      // 如果pay还没来，那么把receipt存入状态，注册一个定时器等待3秒
      receiptState.update(value)
      ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 3000L)
    }
  }

  // 定时器触发，有两种情况，所以要判断当前有没有pay和receipt
  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 如果pay不为空，说明receipt没来，输出unmatchedPays
    if (payState.value() != null) {
      ctx.output(unMatchedPays, payState.value())
    }
    if (receiptState.value() != null) {
      ctx.output(unMatchedReceipts, receiptState.value())
    }
    //清空状态
    payState.clear()
    receiptState.clear()
  }
}
