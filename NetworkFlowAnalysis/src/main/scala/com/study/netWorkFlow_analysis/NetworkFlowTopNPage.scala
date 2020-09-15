package com.study.netWorkFlow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Map

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.optimizer.operators.MapDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

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
    val lateOutPutTag: OutputTag[ApacheLogEvent] = new OutputTag[ApacheLogEvent]("lata data")
    val aggStream: DataStream[PageViewCount] = dataStream
      .keyBy(_.url) //这种方式比指定字段名要好
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(lateOutPutTag)
      .aggregate(new PageCountAgg(), new PageCountWindow())

    //得到侧输出流
    val lateOutPutStream: DataStream[ApacheLogEvent] = aggStream.getSideOutput(lateOutPutTag)

    //每个窗口的统计值排序输出
    val resultStream: DataStream[String] = aggStream
      .keyBy(_.windowEnd)
      .process(new PageViewCountTopN(5))

    resultStream.print()
    environment.execute("topN page job")
  }
}

//自定义预聚合函数
class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义windowFunction，包装成样例类输出
class PageCountWindow() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, window.getEnd, input.head))
  }
}

//自定义Process Function
class PageViewCountTopN(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {
  //定义MapState保存所有聚合结果，使用map结构可以防止一个url保存多个结果
  lazy val pageCountState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pagecount-map", classOf[String], classOf[Long]))

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
    pageCountState.put(value.url, value.count)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    //注册清空状态数据的定时器，1分钟以后清除状态，因为 .allowedLateness( Time.minutes(1) )设置了1分钟
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60 * 1000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //当时间到达了最大的延迟时间时，清空当前窗口的状态，进行下一个窗口
    if (timestamp == ctx.getCurrentKey + 60 * 1000L) {
      pageCountState.clear()
      return
    }

    val allPageCountList: ListBuffer[(String, Long)] = new ListBuffer[(String, Long)]
    val iter: util.Iterator[Map.Entry[String, Long]] = pageCountState.entries().iterator()
    while (iter.hasNext) {
      val map: Map.Entry[String, Long] = iter.next()
      allPageCountList += ((map.getKey, map.getValue))
    }

    //排序取前n个
    val sortedPageCountList: ListBuffer[(String, Long)] = allPageCountList.sortWith(_._2 > _._2).take(n)

    val result: StringBuilder = new StringBuilder
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    //遍历sorted列表，取出topN信息
    for (i <- sortedPageCountList.indices) {
      //获取当前商品信息
      val currentItemCount: (String, Long) = sortedPageCountList(i)
      result.append("Top:").append(i + 1).append(":").append("  页面url=").append(currentItemCount._1)
        .append(" 访问量：").append(currentItemCount._2).append("\n")
    }
    result.append("=====================\n")
    //控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
