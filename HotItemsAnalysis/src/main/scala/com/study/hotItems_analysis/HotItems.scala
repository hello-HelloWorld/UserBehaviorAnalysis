package com.study.hotItems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/*
* @author: helloWorld
* @date  : Created in 2020/8/25 14:51
*/
/*
* 实时热门商品
* 每隔5分钟输出最近一小时内点击量最多的前N个商品
* */

/*定义输入数据的样例类
userId	Long	加密后的用户ID
itemId	Long	加密后的商品ID
categoryId	Int	加密后的商品所属类别ID
behavior	String	用户行为类型，包括(‘pv’, ‘’buy, ‘cart’, ‘fav’)
timestamp	Long	行为发生的时间戳，单位秒*/
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//定义窗口聚合结果的样例类
case class ItemViewCount(itemId: Long, windowWnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    //设置时间语义为事件时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //从文件读取数据
    val inputStream: DataStream[String] = environment.readTextFile("E:\\workspace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    //从kafka读取数据
    val properties: Properties = new Properties()
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    //    val inputStream: DataStream[String] = environment.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))

    //将datastream封装成样例类，并将timestamp设置为watermark
    val dataStream: DataStream[UserBehavior] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000) //时间是有序的

    //对数据进行转换，过滤出pv行为，开窗聚合，得到各个窗口中的每个itemid的个数
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv") //过滤出pv行为
      .keyBy("itemId") //按照itemId分组
      .timeWindow(Time.hours(1), Time.minutes(5)) //滑动窗口
      .aggregate(new CountAgg(), new ItemCountWindowFunction())

    //对窗口聚合结果按照窗口分组，并做排序取TopN输出
    val resultStream: DataStream[String] = aggStream
      .keyBy("windowWnd")
      .process(new TopNHotItems(5))

    resultStream.print()
    environment.execute("hot items topN job")
  }
}

// 自定义预聚合函数，来一条数据就加1
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数，结合window信息包装成样例类
class ItemCountWindowFunction() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd: Long = window.getEnd
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

// 自定义 KeyedProcessFunction
class TopNHotItems(n: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  //定义一个ListState，用来保存当前窗口所有的count结果
  lazy val itemCountListState: ListState[ItemViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemcount-liststate", classOf[ItemViewCount]))

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每来一条数据，就把它保存到状态中
    itemCountListState.add(value)
    //注册定时器，触发的时间为windowEnd+100
    ctx.timerService().registerEventTimeTimer(value.windowWnd + 100)
  }

  //定时器触发时，从状态中取数据，然后排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //先把状态中的数据提取到一个ListBuffer中
    val allIteamCountList: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (itemCount <- itemCountListState.get()) {
      allIteamCountList += itemCount
    }
    //按照count值大小排序，取topN
    val sortedItemCountList: ListBuffer[ItemViewCount] = allIteamCountList.sortBy(_.count)(Ordering.Long.reverse).take(n)

    //清除状态
    itemCountListState.clear()

    //将排名信息格式化成string，方便监控显示
    val result: StringBuilder = new StringBuilder
    result.append("时间：").append(new Timestamp(timestamp - 100)).append("\n")
    //遍历sorted列表，输出topN信息
    for (i <- sortedItemCountList.indices) {
      //获取当前商品信息
      val currentItemViewCount: ItemViewCount = sortedItemCountList(i)
      result.append("Top").append(i + 1).append(":").append(" 商品ID=").append(currentItemViewCount.itemId).append("  访问量=").append(currentItemViewCount.count).append("\n")
    }
    result.append("=====================\n")

    //控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
