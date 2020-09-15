package com.study.market_analysis

import java.net.URL
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
* @author: helloWorld
* @date  : Created in 2020/9/4 9:54
*/
/*
* 页面广告按照省份划分的点击量的统计
* */

// 定义输入输出样例类
case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class AdCountByProvince(province: String, windowEnd: String, count: Long)

//定义测输出流报警信息样例类
case class BlaclListWarning(userId: Long, adId: Long, msg: String)

object AdAnalysisByProvince {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    //设置时间语义为事件时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据文件
    val resource: URL = getClass.getResource("/AdClickLog.csv")
    val inputStream: DataStream[String] = environment.readTextFile(resource.getPath)
    //    val inputStream: DataStream[String] = environment.readTextFile("E:\\workspace\\UserBehaviorAnalysis\\MarketAnalysis\\src\\main\\resources\\AdClickLog.csv")
    //将数据封装为样例类
    val dataStream: DataStream[AdClickEvent] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      AdClickEvent(dataArr(0).toLong, dataArr(1).toLong, dataArr(2), dataArr(3), dataArr(4).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    /*
    *定义刷单行为过滤操作
    * 对一段时间内（比如一天内）的用户点击行为进行约束，
    * 如果对同一个广告点击超过一定限额（比如100次），应该把该用户加入黑名单并报警，此后其点击行为不应该再统计
    * */
    val filterStream: DataStream[AdClickEvent] = dataStream
      .keyBy(data => (data.userId, data.adId)) //按照用户和广告id分组
      .process(new FilterBlackList(100L))


    //按照province分组，开窗聚合统计
    val resultStream: DataStream[AdCountByProvince] = filterStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())

    resultStream.print()
    filterStream.getSideOutput(new OutputTag[BlaclListWarning]("blackList")).print("blackList")
    environment.execute("ad job")
  }
}

//自定义预聚合函数
class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdCountResult() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {
    out.collect(AdCountByProvince(key, new Timestamp(window.getEnd).toString, input.head))
  }
}

// 实现自定义的ProcessFunction，判断用户对广告的点击次数是否达到上限
class FilterBlackList(maxClickCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {
  //定义状态，保存当前用户对当前广告的点击量count
  lazy val count: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  //标识位，用来表示当前用户是否已经在黑名单中
  lazy val isBlackState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is_black", classOf[Boolean]))

  override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
    //取出状态数据
    val curCount: Long = count.value()

    // 如果是第一个数据，那么注册第二天0点的定时器，用于清空状态
    if (curCount == 0) {
      val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    // 判断count值是否达到上限，如果达到，并且之前没有输出过报警信息，那么报警
    if (curCount >= maxClickCount) {
      if (!isBlackState.value()) {
        ctx.output(new OutputTag[BlaclListWarning]("blackList"), BlaclListWarning(value.userId, value.adId, "clicl over " + maxClickCount + " times today"))
        isBlackState.update(true)
      }
      return
    }
    //count值加1
    count.update(curCount + 1)
    out.collect(value)
  }

  //0点触发定时器，直接清空状态
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
    count.clear()
    isBlackState.clear()
  }
}
