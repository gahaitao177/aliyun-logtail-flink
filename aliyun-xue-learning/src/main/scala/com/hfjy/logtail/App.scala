package com.hfjy.logtail

import com.aliyun.openservices.log.flink.FlinkLogConsumer
import com.aliyun.openservices.log.flink.data.{RawLog, RawLogGroup, RawLogGroupList, RawLogGroupListDeserializer}
import com.hfjy.logtail.bean.Business
import com.hfjy.logtail.flink.SourceSink
import com.hfjy.logtail.util.ConfigUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup

import scala.collection.JavaConverters._

/**
  * Created by kehailin on 2018-11-15.
  *
  * Project: study
  * Logstore: xue-gateway-learning_learning
  * 对应离线数据： learning
  */
object App {
    def main(args: Array[String]): Unit = {

        val tool: ParameterTool = ParameterTool.fromArgs(args)
//        if (!checkArguments(tool)){
//            System.err.println("No port specified. Please run 'App --output <output>'")
//            System.exit(1)
//        }

        val (configProps, deserializer) = ConfigUtil.getConfig

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.enableCheckpointing(60000)
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
        env.getCheckpointConfig.setCheckpointTimeout(60000)
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
        env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        val checkpoint = "hdfs://hfflink/flink/checkpoints/xue_learning"
//        env.setStateBackend(new RocksDBStateBackend(checkpoint, true))

        val inputStream = env.addSource(new FlinkLogConsumer[RawLogGroupList](deserializer, configProps))

        val stream = new DataStream[RawLogGroupList](inputStream)  //转化成Scala的DataStream
        val result = transform(stream)

//        result.addSink(new SourceSink[Business](tool, "aliyun_xue_learning").elasticSearchSink())

        result.print().setParallelism(1)
        env.execute("aliyun_xue_learning")
    }




    def transform(stream: DataStream[RawLogGroupList]): DataStream[Business] = {
        val rawLogGroup: DataStream[RawLogGroup] = stream.flatMap(r => {
            val list = r.rawLogGroups.asScala
            list.iterator
        })

        val rawLog: DataStream[RawLog] = rawLogGroup.flatMap(r => {
            val list = r.getLogs.asScala
            list.iterator
        })

        val contents = rawLog.map(_.contents).filter(r => {
            val message = r.get("message")
            (message.contains("RtcEvent")
                || message.contains("RoomEvent")
                || message.contains("RtcList")
                || message.contains(">>>onClose"))
        })

        val result = contents.map(c => {

            try {
                val originDateTime = c.getOrDefault("time", "")
                val dateTime = originDateTime.substring(0, 19)
                val message = c.getOrDefault("message", "")
                val arr = message.split("\t")

                var logType = arr(0).split("-")(1).trim()

                var lessonPlanID = 0
                var courseStatus = ""
                var courseName = ""
                var userID = 0
                var userName = ""
                var userType = -1
                var action = ""
                var currentRtc = ""
                var voiceSetStr = ""
                var videoSetStr = ""
                var channel = ""
                var channelType = -1
                var userAgent = ""
                var deviceID = ""
                var messageSize = 0

                if (logType.contains("RtcEvent")) {
                    logType = "rtcevent"
                    lessonPlanID = arr(1).toInt
                    userID = arr(2).toInt
                    userType = arr(3).toInt
                    userName = arr(4)
                    action = arr(5)
                    currentRtc = arr(6)
                    channel = arr(7)
                    channelType = arr(9).toInt

                } else if (logType.contains("RoomEvent")) {
                    logType = "roomevent"
                    lessonPlanID = arr(1).toInt
                    val lessonUser = arr(2).split("_")
                    userID = lessonUser(1).toInt
                    userType = lessonUser(2).toInt
                    courseStatus = arr(3)
                    courseName = arr(4)
                    action = arr(5)
                    messageSize = arr(6).toInt
                    if (arr(7).contains("Windows")){
                        userAgent = arr(7) + "|" + arr(8)
                    }else {
                        userAgent = arr(7) + "|" + arr(9) + "|" + arr(10)
                    }
                    deviceID = arr(13)

                } else if (logType.contains("RtcList")){
                    logType = "rtclist"
                    val rtcListArr = message.split("\t")
                    lessonPlanID = rtcListArr(1).toInt
                    userID = rtcListArr(2).toInt
                    userType = rtcListArr(3).toInt
                    userName = rtcListArr(4)
                    if (rtcListArr.length == 7) {
                        voiceSetStr = rtcListArr(5)
                        videoSetStr = rtcListArr(6)
                    }

                    if (rtcListArr.length == 6 && message.endsWith("\t")) {
                        voiceSetStr = rtcListArr(5)
                    }

                    if (rtcListArr.length == 6 && !message.endsWith("\t")) {
                        videoSetStr = rtcListArr(5)
                    }

                } else {  //>>>onClose!
                    logType = "onclose"
                    val oncloseArr = message.split(">>>onClose!")(1).trim().split(" ")
                    lessonPlanID = oncloseArr(1).toInt


                    if (!"null".equals(oncloseArr(2))){
                        val lu = oncloseArr(2).split("_")
                        userID = lu(1).toInt
                        userType = lu(2).toInt
                        deviceID = oncloseArr(3)
                    }
                }
                Business(logType,
                    originDateTime,
                    dateTime,
                    lessonPlanID,
                    courseStatus,
                    courseName,
                    userID,
                    userName,
                    userType,
                    action,
                    currentRtc,
                    voiceSetStr,
                    videoSetStr,
                    channel,
                    channelType,
                    userAgent,
                    deviceID,
                    messageSize)
            } catch {
                case e: Exception => Business()
            }
        }).filter(b => b.dateTime != "" )

        result
    }

    def checkArguments(tool: ParameterTool): Boolean = {
        tool.has("output")
    }
}
