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
  * Created by kehailin on 2018-12-21. 
  */
object AssigningModel {
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
        val checkpoint = "hdfs://hfflink/flink/checkpoints/ml_trial_lesson_assining_model"
        env.setStateBackend(new RocksDBStateBackend(checkpoint, true))

        val inputStream = env.addSource(new FlinkLogConsumer[RawLogGroupList](deserializer, configProps))

        val stream = new DataStream[RawLogGroupList](inputStream)  //转化成Scala的DataStream
        val assigningModel: DataStream[RawLog] = initTransform(stream)


        val modelAssign = transformAssigningModel(assigningModel)

        modelAssign.addSink(new SourceSink[AssigningModelTeacherId](tool, "aliyun_ml_trial_lesson_recommend_assigin_model").elasticSearchSink())

        env.execute("aliyun_ml_trial_lesson_assigining_model")

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
