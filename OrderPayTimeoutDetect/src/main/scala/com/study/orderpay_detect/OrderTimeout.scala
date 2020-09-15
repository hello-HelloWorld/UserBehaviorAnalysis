package com.study.orderpay_detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/*
* @author: helloWorld
* @date  : Created in 2020/9/4 17:43
*/
/*
* 订单支付超时监控--对订单状态进行监控，设置一个失效时间（比如15分钟），如果下单后一段时间仍未支付，订单就会被取消
* */
// 定义输入输出数据的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val path: String = getClass.getResource("/OrderLog.csv").getPath
    val inputStream: DataStream[String] = environment.readTextFile(path)

    val dataStream: DataStream[OrderEvent] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })

    //1.定义一个要匹配事件序列的模式
    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create") //首先是订单的create事件
      .followedBy("pay").where(_.eventType == "pay") //后面来的是订单的pay事件
      .within(Time.minutes(15))

    //2.将pattern应用在按照orderId分组的数据流
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(dataStream.keyBy(_.orderId), orderPayPattern)

    // 3. 定义一个侧输出流标签，用来标明超时事件的侧输出流
    val orderTimeOutTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("order timeOut")

    // 4. 调用select方法，提取匹配事件和超时事件，分别进行处理转换输出
    val resultStream: DataStream[OrderResult] = patternStream
      .select(orderTimeOutTag, new OrderTimeoutSelect(), new OrderPaySelect())

    resultStream.print()
    resultStream.getSideOutput(orderTimeOutTag).print("timeout")
    environment.execute("order pay job")
  }
}

// 自定义超时处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeOutOrderId: Long = map.get("create").iterator().next().orderId
    OrderResult(timeOutOrderId, "timeout at " + l)
  }
}

// 自定义匹配处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val id: Long = map.get("pay").get(0).orderId
    OrderResult(id, "pay successfully")
  }
}







