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
  private var bootstrap_servers = "10.45.136.147:9093,10.45.136.162:9093,10.45.136.38:9093"
  
  def main(args: Array[String]) {
    consume2()
  }
  
  def consume2(){
    import scala.collection.JavaConverters._
    import org.apache.kafka.clients.consumer.KafkaConsumer
    import org.apache.kafka.common.serialization.StringDeserializer 

    val properties = new java.util.Properties()
    properties.put("bootstrap.servers", bootstrap_servers)
    properties.put("group.id", "consumer")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[StringDeserializer])
    
    /*configure the following three settings for SSL Encryption */
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    //properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "C:/Users/ybwang1/ssl/kafka.truststore");
    properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "kafka.truststore");
    properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,  "autickerStorePWD");

    /* configure the following three settings for SSL Authentication */
    //properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "C:/Users/ybwang1/ssl/generalClient.keystore");
    properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "generalClient.keystore");
    properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "autickerStorePWD");
    properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "autickerKeyPWD");    
    properties.setProperty("ssl.endpoint.identification.algorithm", "")

    val kafkaConsumer = new KafkaConsumer[String, String](properties)
    kafkaConsumer.subscribe(java.util.Arrays.asList("test4"))

    while (true) {
      val results = kafkaConsumer.poll(2000).asScala
      println(s"size of results is ${results.size}")
      
      for(record <- results){
        println(record.value())
      }
    }    
  }

}