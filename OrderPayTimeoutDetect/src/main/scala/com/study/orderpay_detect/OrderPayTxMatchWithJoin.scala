package com.study.orderpay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/*
* @author: helloWorld
* @date  : Created in 2020/9/8 16:52
*/

object OrderPayTxMatchWithJoin {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从文件中读取数据，并转换成样例类
    val resource1 = getClass.getResource("/OrderLog.csv")
    val orderEventStream: KeyedStream[OrderEvent, String] = environment.readTextFile(resource1.getPath)
      //      val orderEventStream = env.socketTextStream("localhost", 7777)
      .map(data => {
      val dataArray = data.split(",")
      OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })
      .filter(_.txId != "") // 只过滤出pay事件
      .keyBy(_.txId)

    val resource2 = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream: KeyedStream[ReceiptEvent, String] = environment.readTextFile(resource2.getPath)
      //      val orderEventStream = env.socketTextStream("localhost", 7777)
      .map(data => {
      val dataArray = data.split(",")
      ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(3)) {
        override def extractTimestamp(element: ReceiptEvent): Long = element.timestamp * 1000L
      })
      .keyBy(_.txId)

    // 使用join连接两条流
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream
      .intervalJoin(receiptEventStream)
      .between(Time.seconds(-3), Time.seconds(5))
      .process(new OrderPayTxDetectWithJoin())

    resultStream.print()
    environment.execute("order pay tx match with join job")

  }
}

// 自定义ProcessJoinFunction
class OrderPayTxDetectWithJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect(left, right)
  }
}
