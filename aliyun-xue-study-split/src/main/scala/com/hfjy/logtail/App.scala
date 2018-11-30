package com.hfjy.logtail

import com.aliyun.openservices.log.flink.FlinkLogConsumer
import com.aliyun.openservices.log.flink.data.{RawLog, RawLogGroup, RawLogGroupList}
import com.hfjy.logtail.bean.{Content, StudyAccess, StudyLessonQuizId, StudyRobotPad}
import com.hfjy.logtail.flink.SourceSink
import com.hfjy.logtail.util.ConfigUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream}
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Created by kehailin on 2018-11-27.
  *
  * 三个业务： access、roborpad、lessonPlanId与quizId对应关系
  */
object App {
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
        val checkpoint = "hdfs://hfflink/flink/checkpoints/xue_study_split"
        env.setStateBackend(new RocksDBStateBackend(checkpoint, true))

        val inputStream = env.addSource(new FlinkLogConsumer[RawLogGroupList](deserializer, configProps))

        val stream = new DataStream[RawLogGroupList](inputStream)  //转化成Scala的DataStream
        val content = transform(stream)

        //study日志目前知道的有三种形式
        val split: SplitStream[Content] = content.split(d => {
            val logmessage = d.logmessage
            logmessage match {
                case log if log.contains("ACCESS") => List("access")
                case log if log.contains("[user-use-tablet]") => List("robotPad")
                case log if log.contains("设置课件题目数量>>>>") => List("lessonPlanIdQuiz")
                case _ => List("others")
            }
        })

        val access = split.select("access")
        val robotPad = split.select("robotPad")
        val lessonPlanIdQuiz = split.select("lessonPlanIdQuiz")

        val studyAccess = transformAccess(access)
        val studyRobotPad = transformRobotPad(robotPad)
        val studyLessonPlanIdQuiz = transformLessonPlanIdQuiz(lessonPlanIdQuiz)

        studyAccess.addSink(new SourceSink[StudyAccess](tool, "aliyun_study_access").elasticSearchSink())
        studyRobotPad.addSink(new SourceSink[StudyRobotPad](tool, "aliyun_study_robotpad").elasticSearchSink())
        studyLessonPlanIdQuiz.addSink(new SourceSink[StudyLessonQuizId](tool, "aliyun_study_lesson_plan_id_quiz").elasticSearchSink())

        env.execute("aliyun_xue_study_split")
    }

    /**
      * 初次转换
      */
    def transform(stream: DataStream[RawLogGroupList]): DataStream[Content] = {
        val rawLogGroup: DataStream[RawLogGroup] = stream.flatMap(r => {
            val list = r.rawLogGroups.asScala
            list
        })

        val rawLog: DataStream[RawLog] = rawLogGroup.flatMap(r => {
            val list = r.getLogs.asScala
            list
        })

        val result = rawLog.map(r => {
            try {
                val content = r.getContents
                val logmessage = content.getOrDefault("message", "")
                val time = content.getOrDefault("time", "")
                Content(time, logmessage)
            } catch {
                case e: Exception => Content()
            }
        }).filter(_.dateTime != "")
        result

    }

    /**
      * study access
      */
    def transformAccess(access: DataStream[Content]): DataStream[StudyAccess] = {
        val result = access.map(d => {
            val dateTime = d.dateTime
            val logmessage = d.logmessage
            try {
                val arr = logmessage.split("\t")
                val httpStatus = arr(1)
                val ip = arr(2)
                val action = arr(3)
                val method = arr(4)
                val uri = arr(5)
                val params = arr(6)
                val referer = arr(7)
                val ua = arr(8)

                val uuid = arr(9)
                val headers = arr(10)
                val responseSize = arr(11)
                val takeTime = arr(12)
                val sessionDevice = arr(13)
                StudyAccess(
                    dateTime,
                    httpStatus,
                    ip,
                    action,
                    method,
                    uri,
                    params,
                    referer,
                    ua,
                    uuid,
                    headers,
                    responseSize,
                    takeTime,
                    sessionDevice
                )
            } catch {
                case e: Exception =>
                    println("Parse Error: logmessage: [" + logmessage + "], exception: " + e)
                    StudyAccess(dateTime = "-1")
            }
        }).filter(_.dateTime != "-1")
        result
    }

    /**
      * robotpad
      */
    def transformRobotPad(robotPad: DataStream[Content]): DataStream[StudyRobotPad] = {
        val result = robotPad.map(d => {
            val dateTime = d.dateTime
            val logmessage = d.logmessage
            try {
                val arr = logmessage.split("\t")
                val lessonPlanId = arr(1).toInt
                val userId = arr(2).toInt
                val userType = arr(3).toInt
                val deviceVersion = arr(4)
                val deviceNodeNo = arr(5)
                val orderVersion = arr(6)
                val deviceSdkVersion = arr(7)
                StudyRobotPad(dateTime,
                    lessonPlanId,
                    userId,
                    userType,
                    deviceVersion,
                    deviceNodeNo,
                    orderVersion,
                    deviceSdkVersion
                )
            } catch {
                case e: Exception => StudyRobotPad(dateTime = "-1")
            }
        }).filter(_.dateTime != "-1")
        result
    }

    /**
      * lesson_plan_id和quiz_id对应关系
      */
    def transformLessonPlanIdQuiz(lessonPlanIdQuiz: DataStream[Content]): DataStream[StudyLessonQuizId] = {
        val result: DataStream[StudyLessonQuizId] = lessonPlanIdQuiz.flatMap(d => {

            val dateTime = d.dateTime
            val logmessage = d.logmessage

            val rows = ListBuffer.empty[StudyLessonQuizId]
            try {
                val arr = logmessage.split("设置课件题目数量>>>>")

                val pattern = "(LessonPlanId)(\\d+)(quizIdslessonPlanQuizIds)(.*)".r
                val (lessonPlanId, quizIds) = arr(1) match {
                    case pattern(_, lpid, _, qid) => (lpid, qid)
                    case _ => ("", "")
                }

                val quizId = quizIds.slice(1, quizIds.length - 1).split(",\\s*")
                for(q <- quizId){
                    rows.append(StudyLessonQuizId(dateTime, lessonPlanId, q))
                }

            } catch {
                case e: Exception => rows.append(StudyLessonQuizId(dateTime = "-1"))
            }
            rows.iterator
        }).filter(_.dateTime != "-1")
        result
    }

    def checkArguments(tool: ParameterTool): Boolean = {
        tool.has("output")
    }
}
