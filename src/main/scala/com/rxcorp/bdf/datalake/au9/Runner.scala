package com.rxcorp.bdf.datalake.au9

import org.apache.kafka.clients._
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.config._

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClients
import com.google.gson.Gson
import org.apache.http.entity.StringEntity

final case class HeartBeat(clientCode: String, datetime: String)

object Runner {
  var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private var bootstrap_servers = "10.45.136.147:9093,10.45.136.162:9093,10.45.136.38:9093"
  private var topic = "app1"
  
  def main(args: Array[String]) {
    consume_gazelle()
  }

  def produce_plaintext() = {
    var producerProperties = new java.util.Properties()
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.45.138.145:9092");
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    producerProperties.put(ProducerConfig.ACKS_CONFIG, "1");

    
    val producer = new KafkaProducer[String,String](producerProperties)
    
    var now = new Date()
    var nowString = dateFormat.format(now)
    var record = new ProducerRecord[String,String](topic, s"Application started at ${nowString}")
    producer.send(record,null).get
    
    for(i <- 1 to 100){
      now = new Date()
      nowString = dateFormat.format(now)
      val record = new ProducerRecord[String,String](topic, s"Application is running at ${nowString}")
      producer.send(record,null).get
      
      println(i)
    }
    
    now = new Date()
    nowString = dateFormat.format(now)
    record = new ProducerRecord[String,String](topic, s"Application completed at ${nowString}")
    producer.send(record,null).get    
  }  
  
  def produce() = {
    var producerProperties = new java.util.Properties()
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    producerProperties.put("ssl.endpoint.identification.algorithm", "")
    producerProperties.put(ProducerConfig.ACKS_CONFIG, "1");
    //producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, appName)
    producerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    //ConsumerProperties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "C:/Users/ybwang1/ssl/kafka.truststore");
    producerProperties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "kafka.truststore");
    producerProperties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,  "autickerStorePWD");

    /* configure the following three settings for SSL Authentication */
    //ConsumerProperties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "C:/Users/ybwang1/ssl/generalClient.keystore");
    producerProperties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "generalClient.keystore");
    producerProperties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "autickerStorePWD");
    producerProperties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "autickerKeyPWD");  
    
    val producer = new KafkaProducer[String,String](producerProperties)
    
    for(i <- 1 to 100){
      val record = new ProducerRecord[String,String](topic, i.toString())
      producer.send(record,null).get
      
      println(i)
    }
  }
  
  def consume_gazelle(){
    import org.apache.kafka.clients.consumer.KafkaConsumer
    import org.apache.kafka.common.serialization.StringDeserializer 
    
    val ConsumerProperties = new java.util.Properties()
    
    ConsumerProperties.put("bootstrap.servers", "cdts10hdbd01d.rxcorp.com:9092")
    ConsumerProperties.put("group.id", "consumer")
    ConsumerProperties.put("key.deserializer", classOf[StringDeserializer])
    ConsumerProperties.put("value.deserializer", classOf[StringDeserializer])
    
    ConsumerProperties.put("auto.offset.reset", "earliest")
    
    // disable the auto commit
    ConsumerProperties.put("enable.auto.commit", "false")
    ConsumerProperties.put("max.poll.records", "1")
    
    
    val kafkaConsumer = new KafkaConsumer[String, String](ConsumerProperties)
    kafkaConsumer.subscribe(java.util.Arrays.asList("helloWorld"))
    
    while(true){
      import scala.collection.JavaConverters._
      val results = kafkaConsumer.poll(1000).asScala
      println(s"size of results is ${results.size}")
      
      if(results.size > 0){
        
        for(result <- results){
          println(result.value())
          //kafkaConsumer.commitSync()
          //return
        }
        
        // commit the offset to Kafka
        //kafkaConsumer.commitSync()
      }
    }     
  }  
  
  def consume(){
    import org.apache.kafka.clients.consumer.KafkaConsumer
    import org.apache.kafka.common.serialization.StringDeserializer 
    
    val ConsumerProperties = new java.util.Properties()
    
    ConsumerProperties.put("bootstrap.servers", bootstrap_servers)
    ConsumerProperties.put("group.id", "consumer")
    ConsumerProperties.put("key.deserializer", classOf[StringDeserializer])
    ConsumerProperties.put("value.deserializer", classOf[StringDeserializer])
    
    ConsumerProperties.put("auto.offset.reset", "earliest")
    
    // disable the auto commit
    ConsumerProperties.put("enable.auto.commit", "false")
    ConsumerProperties.put("max.poll.records", "1")
    
    /*configure the following three settings for SSL Encryption */
    ConsumerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    //ConsumerProperties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "C:/Users/ybwang1/ssl/kafka.truststore");
    ConsumerProperties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "kafka.truststore");
    ConsumerProperties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,  "autickerStorePWD");

    /* configure the following three settings for SSL Authentication */
    //ConsumerProperties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "C:/Users/ybwang1/ssl/generalClient.keystore");
    ConsumerProperties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "generalClient.keystore");
    ConsumerProperties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "autickerStorePWD");
    ConsumerProperties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "autickerKeyPWD");    
    ConsumerProperties.setProperty("ssl.endpoint.identification.algorithm", "")
    
    val kafkaConsumer = new KafkaConsumer[String, String](ConsumerProperties)
    kafkaConsumer.subscribe(java.util.Arrays.asList(topic))
    
    while(true){
      import scala.collection.JavaConverters._
      val results = kafkaConsumer.poll(1000).asScala
      println(s"size of results is ${results.size}")
      
      if(results.size > 0){
        
        for(result <- results){
          println(result.value())
          //kafkaConsumer.commitSync()
          //return
        }
        
        // commit the offset to Kafka
        //kafkaConsumer.commitSync()
      }
    }     
  } 
}