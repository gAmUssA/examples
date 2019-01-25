package io.confluent.examples.clients.cloud.dsl

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.*

class MyConsumer<T, U>(
        private val kafkaConsumer: KafkaConsumer<T, U>,
        private val pollDuration: Duration = Duration.ofMillis(100)
) {

  @Volatile
  var keepGoing = true

  fun consume(handler: (key: T, value: U) -> Unit) {
    Thread(Runnable {
      keepGoing = true
      kafkaConsumer.use {
        while (keepGoing) {
          val records = it.poll(pollDuration)
          for (record in records) {
            handler(record.key(), record.value())
          }
        }
      }
    }).start()
  }

  fun stop() {
    keepGoing = false
  }
}

fun <T, U> consumer(
        listOfTopic: List<String>,
        config: Properties,
        consume: MyConsumer<T, U>.() -> Unit
) {
  MyConsumer(KafkaConsumer<T, U>(config).apply {
    subscribe(listOfTopic)
  }).consume()
}

