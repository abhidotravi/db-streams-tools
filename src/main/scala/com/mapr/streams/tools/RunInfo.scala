package com.mapr.streams.tools

private [tools] case class RunInfo(tableName: String,
                                   streamName: String,
                                   topicName: String,
                                   partition: Int,
                                   maxMsg: Long = 10)