package com.study.netWorkFlow_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
* @author: helloWorld
* @date  : Created in 2020/9/2 16:12
*/

/*
* 网站的独立访客数（Unique Visitor，UV）的统计
* 统计埋点日志中的 pv 行为，利用 Set 数据结构进行去重
* */

//定义输出样例类
case class UvCount(windowEnd: Long, count: Long)

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    //设置时间语义为事件时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取文件数据
    val inputStream: DataStream[String] = environment.readTextFile("E:\\workspace\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")

    //将数据转换成样例类类型，并且提取timestamp设置为watermark
    val dataStream: DataStream[UserBehavior] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L) //文件数据为升序

    //分配key，包装成二元组，开窗聚合
    val resultStream: DataStream[UvCount] = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1)) //开个1小时滚动窗口进行统计
      //      .apply(new UvCountRsult())
      .aggregate(new UvCountAgg(), new UvCountResultWithIncreAgg())

    resultStream.print()
    environment.execute("uv job")
  }
}

//自定义全窗口函数
class UvCountRsult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //定义一个set类型来保存所有的userId,自动去重
    var userIdSet: Set[Long] = Set[Long]()
    //将当前窗口所有的数据，添加到set中
    for (userBehavior <- input) {
      userIdSet += userBehavior.userId
    }
    //输出set的大小，就是去重之后的UV值
    out.collect(UvCount(window.getEnd, userIdSet.size))
  }
}

// 自定义增量聚合函数，需要定义一个Set作为累加状态
class UvCountAgg() extends AggregateFunction[UserBehavior, Set[Long], Long] {
  override def createAccumulator(): Set[Long] = Set[Long]()

  override def add(value: UserBehavior, accumulator: Set[Long]): Set[Long] = accumulator + value.userId

  override def getResult(accumulator: Set[Long]): Long = accumulator.size

  override def merge(a: Set[Long], b: Set[Long]): Set[Long] = a ++ b
}

// 自定义窗口函数，添加window信息包装成样例类
class UvCountResultWithIncreAgg() extends AllWindowFunction[Long, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[Long], out: Collector[UvCount]): Unit = {
    out.collect(UvCount(window.getEnd(), input.head))
  }
}

