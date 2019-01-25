package io.confluent.examples.clients.cloud.dsl

import io.confluent.examples.clients.cloud.model.DataRecord
import io.confluent.examples.clients.cloud.util.loadConfig
import io.confluent.kafka.serializers.KafkaJsonDeserializer
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig.JSON_VALUE_TYPE
import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration

fun main(args: Array<String>) {

  //region Program argument check
  if (args.size != 2) {
    println("Please provide command line arguments: <configPath> <topic>")
    System.exit(1)
  }
  //endregion

  // Load properties from disk.
  val config = loadConfig(args[0])
  val topic = args[1]

  //region Add additional properties
  config[KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
  config[VALUE_DESERIALIZER_CLASS_CONFIG] = KafkaJsonDeserializer::class.java.name
  config[JSON_VALUE_TYPE] = DataRecord::class.java
  config[GROUP_ID_CONFIG] = "kotlin_example_group_1"
  config[AUTO_OFFSET_RESET_CONFIG] = "earliest"
  //endregion

  var totalCount = 0L
  consumer<String, DataRecord>(listOf(topic), config) {
    consume { key, value ->
      totalCount += value.count
      println("Consumed record with key $key and value $value, and updated total count to $totalCount")
    }
    Runtime.getRuntime().addShutdownHook(Thread(Runnable {
      stop()
    }))
  }
}

