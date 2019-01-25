package io.confluent.examples.clients.cloud.dsl

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

class MyProducer<T, U>(private val kafkaProducer: KafkaProducer<T, U>, private val topic: String) {

  fun send(key: T, value: U, callback: Callback) {
    kafkaProducer.send(ProducerRecord(topic, key, value), callback)
  }

  fun flush() = kafkaProducer.flush()
}

fun <T, U> producer(topic: String, config: Properties, produce: MyProducer<T, U>.() -> Unit) {
  MyProducer<T, U>(KafkaProducer(config), topic).produce()
}
