package com.hfjy.ml

import com.alibaba.fastjson.JSON
import com.aliyun.openservices.log.flink.FlinkLogConsumer
import com.aliyun.openservices.log.flink.data.{RawLog, RawLogGroup, RawLogGroupList}
import com.hfjy.ml.bean._
import com.hfjy.ml.flink.SourceSink
import com.hfjy.ml.util.ConfigUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.JavaConverters._
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend

import scala.collection.mutable.ListBuffer

/**
  * Created by kehailin on 2018-12-14. 
  */
object TrialLessonResponse {
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
        val checkpoint = "hdfs://hfflink/flink/checkpoints/ml_trial_lesson_response"
        env.setStateBackend(new RocksDBStateBackend(checkpoint, true))

        val inputStream = env.addSource(new FlinkLogConsumer[RawLogGroupList](deserializer, configProps))

        val stream: DataStream[RawLogGroupList] = new DataStream[RawLogGroupList](inputStream)  //转化成Scala的DataStream
        val init = initTransform(stream)

        val responses = init.split(d => {
            val message = d.contents.get("message")
            message match {
                case msg if msg.contains("batches") => List("batches")
                case msg if msg.contains("lesson_begin_time") => List("lesson_begin_time")
                case _ => List("others")
            }
        })

        val batches: DataStream[RawLog] = responses.select("batches")
        val lessonBeginTime = responses.select("lesson_begin_time")

        val batchResponse = transformBatches(batches)
        val lessonResponse = transformLesson(lessonBeginTime)

        batchResponse.addSink(new SourceSink[BatchResponse](tool, "aliyun_ml_trial_lesson_batch_response").elasticSearchSink())
        lessonResponse.addSink(new SourceSink[LessonResponse](tool, "aliyun_ml_trial_lesson_lesson_response").elasticSearchSink())

        env.execute("aliyun_ml_trial_lesson_response")

    }

    def initTransform(stream: DataStream[RawLogGroupList]): DataStream[RawLog] = {
        val rawLogGroup: DataStream[RawLogGroup] = stream.flatMap(r => {
            val list = r.rawLogGroups.asScala
            list.iterator
        })

        val rawLog: DataStream[RawLog] = rawLogGroup.flatMap(r => {
            val list = r.getLogs.asScala
            list.iterator
        })

        rawLog
    }

    def transformLesson(rawLog: DataStream[RawLog]): DataStream[LessonResponse] = {

        val contents = rawLog.map(_.contents).filter(_.get("level") == "INFO")
        val response = contents.map(line => {
            try {
                val dateTime = line.get("time")
                val info = line.get("message").trim
                val msg = JSON.parseObject(info, classOf[Message])
                val code = msg.code
                val message = msg.message
                val dataStr = msg.data
                val data = JSON.parseObject(dataStr, classOf[LessonData])
                val lessonBeginTime = data.lesson_begin_time
                val orderId = data.order_id
                val noticeType = data.notice_type
                val teacherId = data.teacher_id.get(0)
                LessonResponse(dateTime, lessonBeginTime, orderId,  teacherId,code, noticeType, message)

            } catch {
                case e: Exception => LessonResponse()
            }

        }).filter(_.dateTime != "")
        response
    }


    def transformBatches(rawLog: DataStream[RawLog]): DataStream[BatchResponse] = {

        val contents = rawLog.map(_.contents).filter(_.get("level") == "INFO")

        val response = contents.flatMap(line => {
            val list = ListBuffer.empty[BatchResponse]

            try {
                val dateTime = line.get("time")
                val info = line.get("message").trim
                val msg = JSON.parseObject(info, classOf[Message])
                val code = msg.code
                val message = msg.message
                val dataStr = msg.data
                val data = JSON.parseObject(dataStr, classOf[BatchData])
                val orderId = data.order_id
                val noticeType = data.notice_type
                val batchs = data.batches
                for (i <- 0 until batchs.size()) {
                    val batchInfo = batchs.get(i)

                    val batch = batchInfo.batch
                    val teacherIds = batchInfo.teacher_ids
                    val timeInterval = batchInfo.time_interval

                    for (j <- 0 until teacherIds.size()) {
                        val teacherId = teacherIds.get(j)

                        list.append(
                            BatchResponse(dateTime,
                                orderId,
                                code,
                                batch,
                                teacherId,
                                timeInterval,
                                noticeType,
                                message
                            )
                        )

                    }
                }
            } catch {
                case e: Exception => list.append(BatchResponse())
            }
            list
        }).filter(_.dateTime != "")
        response
    }

    def checkArguments(tool: ParameterTool): Boolean = {
        tool.has("output")
    }
}
