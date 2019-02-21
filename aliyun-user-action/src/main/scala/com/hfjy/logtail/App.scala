package com.hfjy.logtail

import com.alibaba.fastjson.JSON
import com.aliyun.openservices.log.flink.FlinkLogConsumer
import com.aliyun.openservices.log.flink.data.{RawLog, RawLogGroup, RawLogGroupList}
import com.hfjy.logtail.bean.Message
import com.hfjy.logtail.flink.SourceSink
import com.hfjy.logtail.util.ConfigUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup

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

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.enableCheckpointing(60000)
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
        env.getCheckpointConfig.setCheckpointTimeout(60000)
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
        env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        val checkpoint = "hdfs://hfflink/flink/checkpoints/user_action"
        env.setStateBackend(new RocksDBStateBackend(checkpoint, true))

        val inputStream = env.addSource(new FlinkLogConsumer[RawLogGroupList](deserializer, configProps))

        val stream: DataStream[RawLogGroupList] = new DataStream[RawLogGroupList](inputStream)  //转化成Scala的DataStream
        val result: DataStream[Message] = transform(stream)

        result.addSink(new SourceSink[Message](tool, "aliyun_user_action").elasticSearchSink())

        result.print()

        env.execute("aliyun_user_action")
    }

    /**
      * 初次转换
      */
    def transform(stream: DataStream[RawLogGroupList]): DataStream[Message] = {
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
                val time = content.getOrDefault("time", "")
                val message = content.getOrDefault("message", "")
                val trimMessage = message.substring(1).trim
                val messageBean = JSON.parseObject(trimMessage, classOf[Message])
                val result = messageBean.copy(dateTime = time)
                result
            } catch {
                case e: Exception =>
                    println("Parse Error, rawLog: " + r + " ,Excption: " + e)
                    Message(dateTime = "-1")
            }
        }).filter(_.dateTime != "-1")
        result

    }

    def checkArguments(tool: ParameterTool): Boolean = {
        tool.has("output")
    }
}
