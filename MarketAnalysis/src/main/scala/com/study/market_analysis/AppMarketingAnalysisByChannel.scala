package com.study.market_analysis

import java.lang
import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/*
* @author: helloWorld
* @date  : Created in 2020/9/3 14:34
*/
/*
* APP市场推广的各个渠道的统计分析
* */

/**
  * @Description: 定义输入数据样例类
  * @param userId    : 用户id
  * @param behavior  : 用户行为
  * @param channel   : 获取渠道
  * @param timestamp : 时间
  **/
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

//定义输出统计的样例类
case class MarketCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

//自定义测试数据源
class SimulateMarketEventSource() extends RichParallelSourceFunction[MarketUserBehavior] {
  //定义是否在运行的标识为
  var running: Boolean = true
  //定义用户行为和推广渠道的列表
  val behaviorSeq: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  val channelSeq: Seq[String] = Seq("appStore", "huaweiStore", "weibo", "wechat")
  //定义随机数生成器
  val random: Random = Random

  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    //定义一个发出数据的最大量，用于控制最大的测试数据量
    val maxCounts = Long.MaxValue
    var count = 0L

    //while循环，不停的随机生成数据
    while (running && count <= maxCounts) {
      val userId: String = UUID.randomUUID().toString
      val behavior: String = behaviorSeq(random.nextInt(behaviorSeq.size))
      val channel: String = channelSeq(random.nextInt(channelSeq.size))
      val timestamp: Long = System.currentTimeMillis()

      ctx.collect(MarketUserBehavior(userId, behavior, channel, timestamp))
      count += 1
      Thread.sleep(50L)
    }
  }
}

object AppMarketingAnalysisByChannel {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    //设置时间语义为事件时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //获取数据
    val inputStream: DataStream[MarketUserBehavior] = environment.addSource(new SimulateMarketEventSource()).assignAscendingTimestamps(_.timestamp)
    val resultStream: DataStream[MarketCount] = inputStream
      .filter(_.behavior != "UNINSTALL") //过滤掉卸载行为
      .keyBy(data => (data.channel, data.behavior)) //按照渠道和行为分组
      .timeWindow(Time.hours(1), Time.seconds(5)) //滑动窗口
      .process(new MarketCountByChannel())

    resultStream.print()
    environment.execute("app")
  }
}

//自定义ProcessWindowFunction全窗口函数
class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior, MarketCount, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketCount]): Unit = {
    val windowStart: String = new Timestamp(context.window.getStart).toString
    val windowEnd: String = new Timestamp(context.window.getEnd).toString
    val channel: String = key._1
    val behavior: String = key._2
    val count: Long = elements.size
    out.collect(MarketCount(windowStart, windowEnd, channel, behavior, count))
  }
}
