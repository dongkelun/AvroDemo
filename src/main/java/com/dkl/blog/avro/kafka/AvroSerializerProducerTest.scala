package com.dkl.blog.avro.kafka

import java.io.File
import java.util.Properties
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.immutable
/**
 * Created by dongkelun on 2021/8/18 14:24
 */
object AvroSerializerProducerTest {

  def main(args: Array[String]): Unit = {

    // Avro Schema解析
    val schema: Schema = new Schema.Parser().parse(new File("src/main/resources/Customer.avsc"))
    println(schema)

    val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

    // 用户数据
    val source = scala.io.Source.fromURL(this.getClass.getResource("/UserBehavior.csv"))
    // 数据解析为User对象
    val data: immutable.Seq[UserBehavior] = source.getLines().toList.map(_.split(","))
      .filter(_.length >= 5)
      .map(arr => UserBehavior(arr))

    // kafka配置参数
    val props = new Properties()
    props.put("bootstrap.servers", "192.168.44.128:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    //创建一个kafka生产者
    val producer: KafkaProducer[String, Array[Byte]] = new KafkaProducer(props)

    //将用户数据写入kafka
    data.foreach(user => {
      val avroRecord: GenericData.Record = new GenericData.Record(schema)
      avroRecord.put("userId", user.userId)
      avroRecord.put("itemId", user.itemId)
      avroRecord.put("categoryId", user.categoryId)
      avroRecord.put("behavior", user.behavior)
      avroRecord.put("timestamp", user.timestamp)
      val bytes = recordInjection.apply(avroRecord)
      try {
        val record = new ProducerRecord[String, Array[Byte]]("user1", bytes)
        producer.send(record).get()
        println(user.toString)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    })

    producer.close()

  }

}
