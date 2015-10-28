package com.yookos.migration

import akka.actor.{ Actor, Props, ActorSystem, ActorRef }
import akka.pattern.{ ask, pipe }
import akka.event.Logging
import akka.util.Timeout

import org.apache.spark._
import org.apache.spark.streaming.{ Milliseconds, Seconds, StreamingContext, Time }
import org.apache.spark.streaming.receiver._

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.mapper._

import org.json4s._
import org.json4s.JsonDSL._
//import org.json4s.native.JsonMethods._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import org.apache.commons.lang.StringEscapeUtils
import org.joda.time.DateTime

/**
 * @author ${user.name}
 */
object App extends App {
  
  // Configuration for a Spark application.
  // Used to set various Spark parameters as key-value pairs.
  val conf = new SparkConf(false) // skip loading external settings
  
  val mode = Config.mode
  Config.setSparkConf(mode, conf)
  val cache = Config.redisClient(mode)
  //val ssc = new StreamingContext(conf, Seconds(2))
  //val sc = ssc.sparkContext
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val system = SparkEnv.get.actorSystem
  
  implicit val formats = DefaultFormats
  
  // serializes objects from redis into
  // desired types
  import com.redis.serialization._
  import Parse.Implicits._
  
  var count = new java.util.concurrent.atomic.AtomicInteger(0)
  val totalLegacyUsers = 2124155L
  var cachedIndex = if (cache.get("latest_idcache_index") == None) 0 else cache.get[Int]("latest_idcache_index").get

  val df = sqlContext.load("jdbc", Map(
    "url" -> Config.dataSourceUrl(mode, Some("mappings")),
    //"dbtable" -> "users")
    "dbtable" -> f"(SELECT id, password, username, email, phonenumber FROM users offset $cachedIndex%d) as users")
  )

  df.select(df("id"), df("password"), df("username"), df("email"), df("phonenumber")).collect().foreach(row => {
    
    cachedIndex = cachedIndex + 1
    cache.set("latest_idcache_index", cachedIndex)
    
    cache.set("idcache:" + row.getString(3), write(EmailKeyedCache(row.getString(0), row.getString(1))))
    cache.set("idcache:" + row.getString(2), "idcache:" + row.getString(3))

    if (row.getString(4) != null) {
      cache.set("idcache:" + row.getString(4), "idcache:" + row.getString(3))
    }

    println("===Latest idcache cachedIndex=== " + cache.get[Int]("latest_idcache_index").get)
  
  })

}

case class EmailKeyedCache(userid: String, password: String) 

case class UserQueryFilter(email: String,
        phonenumber: String) extends java.io.Serializable
