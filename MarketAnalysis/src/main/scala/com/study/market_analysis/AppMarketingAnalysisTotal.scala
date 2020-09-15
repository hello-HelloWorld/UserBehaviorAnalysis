package com.study.market_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
* @author: helloWorld
* @date  : Created in 2020/9/3 17:17
*/
/*
*APP市场推广--得到所有渠道推广的总量。
* */
object AppMarketingAnalysisTotal {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[MarketUserBehavior] = environment.addSource(new SimulateMarketEventSource()).assignAscendingTimestamps(_.timestamp)
    val resultStream: DataStream[MarketCount] = inputStream
      .filter(_.behavior != "INSTALL")
      .map(data => ("total", 1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new MarketCountAgg(), new MarketCountResult())

    resultStream.print()
    environment.execute("app")
  }
}

//自定义预聚合函数
class MarketCountAgg() extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口函数
class MarketCountResult() extends WindowFunction[Long, MarketCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketCount]): Unit = {
    val windowStart: String = new Timestamp(window.getStart).toString
    val windowEnd: String = new Timestamp(window.getEnd).toString
    out.collect(MarketCount(windowStart, windowEnd, "total", "total", input.head))
  }
}
