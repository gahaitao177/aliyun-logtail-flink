package com.hfjy.logtail

import com.alibaba.fastjson.JSON
import com.aliyun.openservices.log.flink.FlinkLogConsumer
import com.aliyun.openservices.log.flink.data.{RawLog, RawLogGroup, RawLogGroupList}
import com.hfjy.logtail.bean.{ClassRoom, LogRoomEvent, LogRtcEvent}
import com.hfjy.logtail.flink.SourceSink
import com.hfjy.logtail.util.ConfigUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.JavaConverters._

/**
  * Created by kehailin on 2018-11-15.
  *
  * Project: learning
  * Logstore: classroom-classroom_log
  * 对应离线数据： learning
  *
  * 从20181211起采用新的json格式
  */
object ClassroomApp {
    def main(args: Array[String]): Unit = {

        val tool: ParameterTool = ParameterTool.fromArgs(args)
        if (!checkArguments(tool)){
            System.err.println("No port specified. Please run 'App --output <output>'")
            System.exit(1)
        }

        val (configProps, deserializer) = ConfigUtil.getConfig

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.enableCheckpointing(60000)
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
        env.getCheckpointConfig.setCheckpointTimeout(60000)
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
        env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        val checkpoint = "hdfs://hfflink/flink/checkpoints/xue_learning"
        env.setStateBackend(new RocksDBStateBackend(checkpoint, true))

        val inputStream = env.addSource(new FlinkLogConsumer[RawLogGroupList](deserializer, configProps))

        val stream = new DataStream[RawLogGroupList](inputStream)  //转化成Scala的DataStream
        val result = transform(stream)

        result.addSink(new SourceSink[ClassRoom](tool, "aliyun_xue_learning").elasticSearchSink())

        env.execute("aliyun_xue_learning")
    }


    def transform(stream: DataStream[RawLogGroupList]): DataStream[ClassRoom] = {
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
            (message.contains("RtcEvent")   //新的日志中只包含 RoomEvent 和 RtcEvent
                || message.contains("RoomEvent"))
        })

        val result = contents.map(c => {

            var dateTime = ""
            var logType = ""
            var lessonPlanId = 0
            var userId: Int = 0
            var userType: Int = -1
            var userName: String = ""
            var action: String = ""
            var currentRtc: String = ""
            var channel: String = ""
            var channelType: String = ""
            var courseStatus: String = ""
            var courseName: String = ""
            var userAgent: String = ""

            try {
                val message = c.get("message").substring(1).trim

                val arr = message.split("( |\t)\\{")
                logType = arr(0)
                val logEvent = "{" + arr(1).trim

                if (logType.contains("RoomEvent")) {
                    logType = "roomevent"
                    val logRoomEvent = JSON.parseObject(logEvent, classOf[LogRoomEvent])
                    dateTime = logRoomEvent.currentTimeStamp
                    lessonPlanId = logRoomEvent.lessonPlanId.toInt
                    val clinetId = logRoomEvent.clientId.split("_")
                    userId = clinetId(1).toInt
                    userType = clinetId(2).toInt
                    courseStatus = logRoomEvent.lessonState
                    courseName = logRoomEvent.lessonPlanName
                    action = logRoomEvent.eventName
                    userAgent = if (logRoomEvent.osName == "") ""
                                else logRoomEvent.osName + "|" + logRoomEvent.clientName + "|" + logRoomEvent.clientVersion

                } else if (logType.contains("RtcEvent")){ //RtcEvent
                    logType = "rtcevent"
                    val logRtcEvent = JSON.parseObject(logEvent, classOf[LogRtcEvent])
                    dateTime = logRtcEvent.currentTimeStamp
                    lessonPlanId = logRtcEvent.lessonPlanId.toInt
                    userId = logRtcEvent.userId.toInt
                    userType = logRtcEvent.userType.toInt
                    userName = logRtcEvent.userName
                    action = logRtcEvent.desc
                    currentRtc = logRtcEvent.source
                    channel = logRtcEvent.label
                    channelType = logRtcEvent.status
                }

                ClassRoom(
                    dateTime,
                    logType,
                    lessonPlanId,
                    userId,
                    userType,
                    userName,
                    action,
                    currentRtc,
                    channel,
                    channelType,
                    courseStatus,
                    courseName,
                    userAgent
                )
            } catch {
                case e: Exception => ClassRoom()
            }
        }).filter(_.dateTime != "")

        result
    }

    def checkArguments(tool: ParameterTool): Boolean = {
        tool.has("output")
    }
}
