package com.study.hotItems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource


/*
* @author: helloWorld
* @date  : Created in 2020/8/26 13:56
*/

object KafkaProducerUtil {

  def main(args: Array[String]): Unit = {
    writeToKafkaWithTopic("hotItems")
  }

  def writeToKafkaWithTopic(topic: String): Unit = {
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //创建一个kafkaProducer，用它来发数据
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)

    //从文件读取数据，逐条发送
    val source: BufferedSource = io.Source.fromFile("E:\\workspace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for (line <- source.getLines()) {
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }
}
