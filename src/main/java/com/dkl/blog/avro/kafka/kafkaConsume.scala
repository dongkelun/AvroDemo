package com.dkl.blog.avro.kafka

import java.io.File
import java.util.{Collections, Properties}

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.ConsumerRecords

/**
 * Created by dongkelun on 2021/8/18 14:29
 */
object kafkaConsume {
  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "192.168.44.128:9092")
    props.put("group.id", "dd3") // 消费组ID
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("auto.offset.reset", "earliest")

    // 创建kafka消费者
    val consumer = new KafkaConsumer[String, Array[Byte]](props)

    // 订阅主题 subscribe() 方法接受一个主题列表作为参数
    // consumer.subscribe("user.*")  也可以使用正则表达式 订阅相关主题
    consumer.subscribe(Collections.singletonList("user"))

    // Avro Schema
    val schema: Schema = new Schema.Parser().parse(new File("src/main/resources/Customer.avsc"))

    val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

    try {
      while (true) {
        val consumerRecords: ConsumerRecords[String, Array[Byte]] = consumer.poll(100) //如果没有数据到consumer buffer 阻塞多久

        for (record <- consumerRecords) {

          //  每条记录都包含了记录所属主题的信息、记录所在分区的信息、记录在分区里的偏移量，以及记录的键值对
          val genericRecord: GenericRecord = recordInjection.invert(record.value()).get
//          val genericRecord: GenericRecord =  record.value().asInstanceOf[GenericRecord]

          println(genericRecord.get("userId") + "\t" +
            genericRecord.get("itemId") + "\t" +
            genericRecord.get("categoryId") + "\t" +
            genericRecord.get("behavior") + "\t" +
            genericRecord.get("timestamp") + "\t")

        }

      }
      // 同步提交 ：在broker对提交请求做出回应之前，应用会一直阻塞
      // 处理完当前批次的消息，在轮询更多的消息之前，
      // 调用 commitSync() 方法提交当前批次最新的偏移量
      consumer.commitAsync()
    } catch {
      case e: Exception => println("Unexpected error", e)
    }
    finally {
      // 异步提交：在成功提交或碰到无法恢复的错误之前，commitSync() 会一直重试，但是commitAsync() 不会
      try {
        consumer.commitSync()
      } finally {
        consumer.close()
      }
    }
  }
}
