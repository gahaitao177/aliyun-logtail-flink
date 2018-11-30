package com.hfjy.logtail

import com.aliyun.openservices.log.flink.FlinkLogConsumer
import com.aliyun.openservices.log.flink.data.{RawLog, RawLogGroup, RawLogGroupList}
import com.hfjy.logtail.bean.Action
import com.hfjy.logtail.flink.SourceSink
import com.hfjy.logtail.util.ConfigUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._

/**
  * Created by kehailin on 2018-11-28. 
  */
object App {
    def main(args: Array[String]): Unit = {

        val tool: ParameterTool = ParameterTool.fromArgs(args)
        if (!checkArguments(tool)){
            System.err.println("No port specified. Please run 'App --output <output>'")
            System.exit(1)
        }

        val (configProps, deserializer) = ConfigUtil.getConfig

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.enableCheckpointing(60000)
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
        env.getCheckpointConfig.setCheckpointTimeout(60000)
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
        env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        val checkpoint = "hdfs://hfflink/flink/checkpoints/learn_action"
        env.setStateBackend(new RocksDBStateBackend(checkpoint, true))

        val inputStream = env.addSource(new FlinkLogConsumer[RawLogGroupList](deserializer, configProps))

        val stream = new DataStream[RawLogGroupList](inputStream)  //转化成Scala的DataStream

        val result = transform(stream, env)

        result.addSink(new SourceSink[Action](tool, "aliyun_learn_action").elasticSearchSink())

        env.execute("aliyun_learn_action")
    }

    /**
      * 初次转换
      */
    def transform(stream: DataStream[RawLogGroupList],
                  env: StreamExecutionEnvironment): DataStream[Action] = {
        val rawLogGroup: DataStream[RawLogGroup] = stream.flatMap(r => {
            val list = r.rawLogGroups.asScala
            list
        })

        val rawLog: DataStream[RawLog] = rawLogGroup.flatMap(r => {
            val list = r.getLogs.asScala
            list
        })

        val startFunctions = List("getQuizsByLessonPlan", "getCacheHomeworkByLessonPlanId", "saveTeacherCorrections", "getAuditionLessonInfo", "saveCourseWare", "saveHomework", "submitTeacherCorrection", "submitAudtionLessonReport", "saveAuditionLessonInfo")
        val endFunctions = List("saveCourseWare", "saveHomework", "submitTeacherCorrection", "submitAudtionLessonReport", "saveAuditionLessonInfo")

        val functions: DataStream[List[String]] = new DataStream(env.fromElements(startFunctions).broadcast())

        val functionMessage = rawLog.map(r => {
            val content = r.getContents
            val time = content.getOrDefault("time", "")
            val message = content.getOrDefault("message", "")
            (time, message)
        }).filter(_._2 != "")
            /*.filter(cm => {
            val msg = cm._2
            val function = msg.split("\t")(0).substring(1).trim
            startFunctions.contains(function)/* || endFunctions.contains(function)*/  //两个functions能在各个slot获取到？还是需要广播变量？
        })*/
            .connect(functions).flatMap(new CoFlatMapFunction[(String, String), List[String], (String, String)] {

                var filterFunction: List[String] = _
                override def flatMap1(value: (String, String), out: Collector[(String, String)]): Unit = {

                    val msg = value._2
                    val function = msg.split("\t")(0).substring(1).trim
                    if (filterFunction.contains(function)) {
                        out.collect(value)
                    }
                }

                override def flatMap2(value: List[String], out: Collector[(String, String)]): Unit = {
                    filterFunction = value
                }
            })

        val result = functionMessage.map(fm => {
            val dateTime = fm._1
            try {
                val fmsg = fm._2.substring(1).trim  //message 都是以"-"开头
                val arr = fmsg.replace("\t\t", "\t").split("\t")

                val function = arr(0)
                val userId = arr(1)
                val userType = arr(2)
                val userName = arr(3)
                val lessonIdName = arr(5)
                val lessonId = arr(6)
                Action(dateTime, function, userId, userType, userName, lessonIdName, lessonId)
            } catch {
                case e: Exception => Action()
            }
        }).filter(_.dateTime != "")

        result

    }


    def checkArguments(tool: ParameterTool): Boolean = {
        tool.has("output")
    }
}
