package com.mapr.streams.tools

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerConfig._

import scala.util.Try

object Utils {
  private[tools] def toInt(o: Option[String]): Option[Int] =
    o.flatMap(s => Try(s.toInt).toOption)

  private[tools] def toLong(o: Option[String]): Option[Long] =
    o.flatMap(s => Try(s.toLong).toOption)

  private[tools] def usage(appName: String): Unit = {
    println(s"Usage: $appName -table </path/to/table> -stream </path/to/stream> [Options]")
    println(s"Options:")
    println(s"-h or -help <for usage> ")
    println(s"-topic <topic name> [default topic: default]")
    println(s"-partition <partition>")
    println(s"-maxmsgs <maximum messages to produce>")
    System.exit(1)
  }

  private[tools] def getVal (args: Array[String], arg: String): Option[String] = {
    if(args.isDefinedAt(args.indexOf(arg)+1))
      Some(args(args.indexOf(arg)+1))
    else
      None
  }

  private[tools] def defaultProperties(): Properties = {
    val props = new Properties()
    props.put(BOOTSTRAP_SERVERS_CONFIG, "dummy")
    props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ACKS_CONFIG, "1")
    props.put("streams.parallel.flushers.per.partition", "false")
    props.put(METADATA_MAX_AGE_CONFIG, "3000")
    props
  }
}
