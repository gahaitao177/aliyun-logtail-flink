package com.hfjy.hmHwl

import com.aliyun.openservices.log.flink.FlinkLogConsumer
import com.aliyun.openservices.log.flink.data.{RawLog, RawLogGroupList}
import com.hfjy.hmHwl.bean.Jpjy
import com.hfjy.hmHwl.flink.SourceSink
import com.hfjy.hmHwl.util.ConfigUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.JavaConverters._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStreamSource
/**
  * Created by kehailin on 2018-12-12. 
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
        val checkpoint = "hdfs://hfflink/flink/checkpoints/learn_hmHwl"
        env.setStateBackend(new RocksDBStateBackend(checkpoint, true))

        val inputStream: DataStreamSource[RawLogGroupList] = env.addSource(new FlinkLogConsumer[RawLogGroupList](deserializer, configProps))

        val stream = new DataStream[RawLogGroupList](inputStream)  //转化成Scala的DataStream
        val result = transform(stream)

        result.addSink(new SourceSink[Jpjy](tool, "aliyun_learn_hmhwl").elasticSearchSink())

        env.execute("aliyun_learn_hmHwl")

    }

    def transform(stream: DataStream[RawLogGroupList]): DataStream[Jpjy] = {
        val rawLogGroup = stream.flatMap(r => {
            val list = r.rawLogGroups.asScala
            list.iterator
        })

        val rawLog: DataStream[RawLog] = rawLogGroup.flatMap(r => {
            val list = r.getLogs.asScala
            list.iterator
        })

        val hwl = rawLog.map(_.contents).map(line => {
            try {
                val dateTime = line.get("time")
                val message = line.get("message").substring(1).trim

                val arr = message.split("\t")
                val jpjyType = arr(0).trim
                val paperId = arr(3)
                val quizId = arr(5)
                val (lessonPlanId, homeWorkId) = if (arr.length == 10) {
                    (arr(7), arr(9))
                } else if (arr.length == 8) {
                    (arr(7), "-1")
                } else {
                    ("-1", "-1")
                }
                Jpjy(dateTime, jpjyType, paperId, quizId, lessonPlanId, homeWorkId)
            } catch {
                case e: Exception => Jpjy()
            }
        }).filter(_.dateTime != "")
        hwl
    }

    def checkArguments(tool: ParameterTool): Boolean = {
        tool.has("output")
    }
}
