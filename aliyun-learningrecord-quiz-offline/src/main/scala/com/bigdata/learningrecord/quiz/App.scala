package com.bigdata.learningrecord.quiz

import com.alibaba.fastjson.JSON
import com.aliyun.openservices.log.flink.FlinkLogConsumer
import com.aliyun.openservices.log.flink.data.{RawLog, RawLogGroup, RawLogGroupList}
import com.bigdata.learningrecord.quiz.bean.LessonQuiz
import com.bigdata.learningrecord.quiz.flink.MysqlSink
import com.bigdata.learningrecord.quiz.util.ConfigUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.collection.JavaConverters._

/**
  * Created by kehailin on 2019-3-29. 
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
        val checkpoint = "hdfs://hfflink/flink/checkpoints/xue_learning_record_quiz_offline"
        env.setStateBackend(new RocksDBStateBackend(checkpoint, true))

        val inputStream = env.addSource(new FlinkLogConsumer[RawLogGroupList](deserializer, configProps))

        val stream = new DataStream[RawLogGroupList](inputStream)  //转化成Scala的DataStream
        val quiz = transform(stream)

        quiz.addSink(new MysqlSink)
        //quiz.print().setParallelism(1)

        env.execute("aliyun_xue_learning_record_quiz_offiline")
    }

    def transform(stream: DataStream[RawLogGroupList]):DataStream[LessonQuiz] =  {
        val rawLogGroup: DataStream[RawLogGroup] = stream.flatMap(r => {
            val list = r.rawLogGroups.asScala
            list.iterator
        }).filter(_.source == "10.111.61.220")

        val rawLog: DataStream[RawLog] = rawLogGroup.flatMap(r => {
            val list = r.getLogs.asScala
            list.iterator
        })

        val contents = rawLog.map(_.contents).filter(_.get("level") == "INFO").filter(_.get("message").contains("toQuiz"))

        val result: DataStream[LessonQuiz] = contents.map(c => {
            try {
                val line = c.get("message").substring(1).trim
                val arr = line.split("(\t| )\\{")
                val userInfo = arr(0).trim
                val command = "{" + arr(1).trim

                val dateTime = c.get("time").substring(0, 23)
                val user = userInfo.split("_")
                val lessonPlanId = user(0).toInt
                val commandJson = JSON.parseObject(command)
                val direct = commandJson.getIntValue("direct")
                val quizId = commandJson.getString("quizId")
                LessonQuiz(dateTime, lessonPlanId, direct, quizId)

            } catch {
                case e: Exception => LessonQuiz()
            }

        }).filter(_.dateTime != "")
        result
    }

    def checkArguments(tool: ParameterTool): Boolean = {
        tool.has("output")
    }


}
