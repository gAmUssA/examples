package io.confluent.examples.clients.cloud.dsl

import io.confluent.examples.clients.cloud.model.DataRecord
import io.confluent.examples.clients.cloud.util.loadConfig
import io.confluent.kafka.serializers.KafkaJsonSerializer
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.ProducerConfig.*
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer

fun main(args: Array<String>) {
  
  //region Program argument check
  if (args.size != 2) {
    println("Please provide command line arguments: configPath topic")
    System.exit(1)
  }
  //endregion

  //region Load properties from file
  val config = loadConfig(args[0])
  config[KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
  config[VALUE_SERIALIZER_CLASS_CONFIG] = KafkaJsonSerializer::class.java
  // TODO: Create topic if needed
  val topic = args[1]
  val numMessages = 10
  //endregion
  
  producer<String, DataRecord>(topic, config) {
    repeat(numMessages) { i ->
      val key = "alice$i"
      val record = DataRecord(i.toLong())
      println("Producing record: $key\t$record")
      send(key, record, Callback { m: RecordMetadata,
                                   _: Exception? ->
        println("Produced record to topic ${m.topic()} partition [${m.partition()}] @ offset ${m.offset()}")
      })
      flush()
    }
  }
}
