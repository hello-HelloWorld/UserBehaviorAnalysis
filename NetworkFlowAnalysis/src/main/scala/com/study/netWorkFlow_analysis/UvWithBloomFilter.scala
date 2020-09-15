package com.study.netWorkFlow_analysis

import java.lang

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/*
* @author: helloWorld
* @date  : Created in 2020/9/2 17:31
*/

/*
* 使用布隆过滤器的UV统计
* 对于超大规模的数据，可以考虑用布隆过滤器进行去重
* */

object UvWithBloomFilter {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    //设置时间语义为事件时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据文件
    val inputStream: DataStream[String] = environment.readTextFile("E:\\workspace\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //分配key，包装成二元组，开窗聚合
    val resultStream: DataStream[UvCount] = dataStream
      .filter(_.behavior == "pv")
      .map(data => ("uv", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger()) //自定义Trigger
      .process(new UvCountResultWithBloomFilter())

    resultStream.print()
    environment.execute("uv bloom job")
  }

}

// 自定义一个触发器，每来一条数据就触发一次窗口计算操作
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {

  // 数据来了之后，触发计算并清空状态，不保存数据，只保留计算结果
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

// 自定义ProcessWindowFunction(全窗口函数)，把当前数据进行处理，位图保存在redis中
class UvCountResultWithBloomFilter() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
  var jedis: Jedis = _
  var bloom: Bloom = _

  override def open(parameters: Configuration): Unit = {
    jedis = new Jedis("localhost", 6379)
    // 位图大小10亿个位，也就是2^30，占用128MB
    bloom = new Bloom(1 << 30)
  }

  // 每来一个数据，主要是要用布隆过滤器判断redis位图中对应位置是否为1
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // bitmap用当前窗口的end作为key，保存到redis里，（windowEnd，bitmap）
    val storedKey: String = context.window.getEnd.toString

    // 我们把每个窗口的uv count值，作为状态也存入redis中，存成一张叫做countMap的表 (windowEnd,count)
    val countMap = "countMap"
    //获取当前的count值
    var count = 0L
    if (jedis.hget(countMap, storedKey) != null)
      count = jedis.hget(countMap, storedKey).toLong

    // 取userId，计算hash值，判断是否在位图中
    val userId: String = elements.last._2.toString
    val offset = bloom.hash(userId, 61)
    val isExist: lang.Boolean = jedis.getbit(storedKey, offset)

    // 如果不存在，那么就将对应位置置1，count加1；如果存在，不做操作
    if (!isExist) {
      jedis.setbit(storedKey, offset, true)
      jedis.hset(countMap, storedKey, (count + 1).toString)
    }
  }
}

// 自定义一个布隆过滤器
class Bloom(size: Long) extends Serializable {
  //定义位图的大小，应该是2的整次幂
  private val cap = size

  //实现一个hash函数
  def hash(str: String, seed: Int): Long = {
    var result = 0
    for (i <- 0 until str.length) {
      result = result * seed + str.charAt(i)
    }
    //返回一个在cap范围内的一个值
    (cap - 1) & result
  }
}