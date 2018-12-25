package com.hfjy.ml

import java.util.concurrent.atomic.AtomicInteger

import com.alibaba.fastjson.JSON
import com.aliyun.openservices.log.flink.FlinkLogConsumer
import com.aliyun.openservices.log.flink.data.{RawLog, RawLogGroup, RawLogGroupList}
import com.hfjy.ml.bean._
import com.hfjy.ml.flink.SourceSink
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
  * Created by kehailin on 2018-12-17. 
  */
object TrialLessonRecommend {
    val ai = new AtomicInteger(0)

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
        val checkpoint = "hdfs://hfflink/flink/checkpoints/ml_trial_lesson_recommend"
        env.setStateBackend(new RocksDBStateBackend(checkpoint, true))

        val inputStream = env.addSource(new FlinkLogConsumer[RawLogGroupList](deserializer, configProps))

        val stream = new DataStream[RawLogGroupList](inputStream)  //转化成Scala的DataStream
        val init: DataStream[RawLog] = initTransform(stream)

        val recommends = init.split(d => {
            val func = d.contents.get("func")
            println("fucntion: " + func)
            func match {
                case "batch_model_a" => List("batch_model_a")
                case "batch_model_b" => List("batch_model_b")
                case "assigning_model" => List("assigning_model")
                case _ => List("others")
            }
        })


        val batchModelA = recommends.select("batch_model_a")
        val batchModelB = recommends.select("batch_model_b")
        val assigningModel = recommends.select("assigning_model")

        val modelA = transformBatchModelA(batchModelA)
        val modelB = transformBatchModelB(batchModelB)
        val modelAssign = transformAssigningModel(assigningModel)

        modelA.addSink(new SourceSink[BatchModelATeacherId](tool, "aliyun_ml_trial_lesson_recommend_model_a").elasticSearchSink())
        modelB.addSink(new SourceSink[BatchModelBTeacherId](tool, "aliyun_ml_trial_lesson_recommend_model_b").elasticSearchSink())
//        modelAssign.addSink(new SourceSink[AssigningModelTeacherId](tool, "aliyun_ml_trial_lesson_recommend_assigin_model").elasticSearchSink())

        env.execute("aliyun_ml_trial_lesson_recommend")

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


    def transformBatchModelA(rawLog: DataStream[RawLog]): DataStream[BatchModelATeacherId] = {
        val contents = rawLog.map(_.contents)

        val model = contents.flatMap(line => {
            val list = ListBuffer.empty[BatchModelATeacherId]
            try {

                println("model a")

                val time = line.get("time")
                val info = line.get("message").trim

                val newInfo = if (!info.endsWith("}]}}")) {
                    val index = info.lastIndexOf("}")
                    info.substring(0, index + 1) + "]}}"
                } else {
                    info
                }

                val msg = JSON.parseObject(newInfo, classOf[Message])
                val version = msg.version
                val code = msg.code
                val message = msg.message
                val data = JSON.parseObject(msg.data, classOf[BatchModelAData])
                val teacherIds = data.teacher_ids.asScala
                for (teacherId <- teacherIds) {
                    val teacher = teacherId.copy(dateTime = time, version = version, code = code, message = message)
                    list.append(teacher)
                }
            } catch {
                case e: Exception => list.append(BatchModelATeacherId())
            }
            list
        }).filter(_.dateTime != "")
        model
    }

    def transformBatchModelB(rawLog: DataStream[RawLog]): DataStream[BatchModelBTeacherId] = {

        val contents = rawLog.map(_.contents)
        val model = contents.flatMap(line => {
            val list = ListBuffer.empty[BatchModelBTeacherId]
            try {

                println("model b")
                val time = line.get("time")
                val info = line.get("message").trim

                val msg = JSON.parseObject(info, classOf[Message])
                val version = msg.version
                val code = msg.code
                val message = msg.message
                val data = JSON.parseObject(msg.data, classOf[BatchModelBData])
                val teacherIds = data.teacher_ids.asScala
                for (teacherId <- teacherIds) {
                    val teacher = teacherId.copy(dateTime = time, version = version, code = code, message = message)
                    list.append(teacher)
                }
            } catch {
                case e: Exception => list.append(BatchModelBTeacherId())
            }
            list
        }).filter(_.dateTime != "")
        model
    }

    def transformAssigningModel(rawLog: DataStream[RawLog]): DataStream[AssigningModelTeacherId] = {

        val contents = rawLog.map(_.contents)

        contents.flatMap(line => {
            val list = ListBuffer.empty[AssigningModelTeacherId]

            try {
                val time = line.get("time")
                val info = line.get("message")

                val jsonArray = JSON.parseArray(info)
                for (i <- 0 until jsonArray.size()) {
                    val data = JSON.parseObject(jsonArray.getString(i), classOf[AssigningModelTeacherId])
                    val teacher = data.copy(dateTime = time)
                    list.append(teacher)
                }
            } catch {
                case e: Exception => list.append(AssigningModelTeacherId())
            }

            list
        }).filter(_.dateTime != "")

    }

    def checkArguments(tool: ParameterTool): Boolean = {
        tool.has("output")
    }
}
