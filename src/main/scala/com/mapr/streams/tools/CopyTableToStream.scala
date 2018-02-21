package com.mapr.streams.tools

import com.mapr.streams.tools.Utils._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.ojai.store.{Connection, DriverManager}

object CopyTableToStream {
  val appName = "CopyTableToStream"
  val connectionURL = "ojai:mapr:"

  def main(args: Array[String]): Unit = {
    try {
      implicit val runInfo: RunInfo = parse(args)
      run
    } catch {
      case e: Throwable => e.printStackTrace();
    }
  }

  private[tools] def parse(args: Array[String]): RunInfo = {
    var table: Option[String] = None
    var stream: Option[String] = None
    var topic: Option[String] = None
    var partition: Option[Int] = None
    var maxMsgs: Option[Long] = None
    args foreach {
      case (value) => value match {
        case "-table" => {
          table = getVal(args, value)
          if (table.isEmpty)
            println("[ERROR]: Value for -table is missing")
        }
        case "-stream" => {
          stream = getVal(args, value)
          if (stream.isEmpty)
            println("[ERROR]: Value for -stream is missing")
        }
        case "-topic" => topic = getVal(args, value)
        case "-partition" => partition = toInt(getVal(args, value))
        case "-maxmsgs" => maxMsgs = toLong(getVal(args, value))

        case "-h" | "-help" => usage(appName)
        case _ => if (value.startsWith("-")) {
          println(s"[ERROR] - Unrecognized argument: $value")
          usage(appName)
        }
      }
    }

    if(table.isEmpty || stream.isEmpty)
      usage(appName)

    RunInfo(table.get,
      stream.get,
      topic.getOrElse("default"),
      partition.getOrElse(-1),
      maxMsgs.getOrElse(-1))
  }

  @throws
  private[tools] def run(implicit runInfo: RunInfo): Unit = {
    val connection: Connection = DriverManager.getConnection(connectionURL)
    val producer = new KafkaProducer[String, String](defaultProperties())

    try {
      val store = connection.getStore(runInfo.tableName)
      val query = if(runInfo.maxMsg > 0)
        connection.newQuery.limit(runInfo.maxMsg).build
      else
        connection.newQuery.build

      val iter = store.find(query).iterator()
      var count = 0
      while(iter.hasNext) {
        val doc = iter.next
        val record = if(runInfo.partition >= 0)
          new ProducerRecord[String, String](runInfo.streamName + ":" + runInfo.topicName,
            runInfo.partition,
            doc.getIdString,
            doc.asJsonString())
        else
          new ProducerRecord[String, String](runInfo.streamName + ":" + runInfo.topicName,
            doc.getIdString,
            doc.asJsonString())

        producer.send(record, new Callback {
          def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
            if(e != null){
              println(s"[ERROR] - Obtained exception for record: ${recordMetadata.toString}")
              e.printStackTrace()
            }
          }
        })

        count += 1
      }
      producer.flush()
      println(s"Total messages produced $count")
    } finally {
      producer.close()
    }
  }
}
