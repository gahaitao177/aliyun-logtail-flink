package com.hfjy.ml

import java.util

import com.alibaba.fastjson.JSON
import com.aliyun.openservices.log.flink.FlinkLogConsumer
import com.aliyun.openservices.log.flink.data.{RawLog, RawLogGroup, RawLogGroupList}
import com.hfjy.ml.bean.{Lesson, LessonRequest}
import com.hfjy.ml.util.ConfigUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.JavaConverters._
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

/**
  * Created by kehailin on 2018-12-13. 
  */
object TrialLessonRequest {

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
        val checkpoint = "hdfs://hfflink/flink/checkpoints/ml_trial_lesson_request"
//        env.setStateBackend(new RocksDBStateBackend(checkpoint, true))

        val inputStream = env.addSource(new FlinkLogConsumer[RawLogGroupList](deserializer, configProps))

        val stream = new DataStream[RawLogGroupList](inputStream)  //转化成Scala的DataStream
        val lesson = transform(stream)

//        result.addSink(new SourceSink[Lesson](tool, "aliyun_ml_trial_lesson_request").elasticSearchSink())
        lesson.print().setParallelism(1)

        env.execute("aliyun_ml_trial_lesson_request")

    }

    def transform(stream: DataStream[RawLogGroupList]): DataStream[Lesson] = {
        val rawLogGroup: DataStream[RawLogGroup] = stream.flatMap(r => {
            val list = r.rawLogGroups.asScala
            list.iterator
        })

        val rawLog: DataStream[RawLog] = rawLogGroup.flatMap(r => {
            val list = r.getLogs.asScala
            list.iterator
        })

        val contents = rawLog.map(_.contents).filter(_.get("level") == "INFO")

        val lesson = contents.flatMap(c => {
            val list = ListBuffer.empty[Lesson]
            try {
                val dateTime = c.get("time")
                val message = c.get("message")
                val request = JSON.parseObject(message, classOf[LessonRequest])
                val figure = request.figures.get(0)
                val count = request.count

                val beginTime = figure.earliest_lesson_begin_time
                val endTime = figure.earliest_lesson_end_time
                val studentId = figure.student_id
                val teacherIds: java.util.List[Int] = figure.teacher_ids
                val orderId = figure.order_id
                for (i <- 0 until teacherIds.size()) {
                    val teacherId = teacherIds.get(i)
                    list.append(Lesson(dateTime, orderId, studentId, teacherId, count, beginTime, endTime))
                }

            } catch {
                case e: Exception => list.append()
            }
            list
        }).filter(_.dateTime != "")
        lesson
    }

    def checkArguments(tool: ParameterTool): Boolean = {
        tool.has("output")
    }

}
