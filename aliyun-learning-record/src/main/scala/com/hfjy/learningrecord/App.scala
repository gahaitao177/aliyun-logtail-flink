package com.hfjy.learningrecord

import com.alibaba.fastjson.JSON
import com.aliyun.openservices.log.flink.FlinkLogConsumer
import com.aliyun.openservices.log.flink.data.{RawLog, RawLogGroup, RawLogGroupList}
import com.hfjy.learningrecord.bean.Record
import com.hfjy.learningrecord.flink.SourceSink
import com.hfjy.learningrecord.util.ConfigUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.JavaConverters._
import org.apache.flink.api.scala._

/**
  * Created by kehailin on 2018-12-11. 
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
        val checkpoint = "hdfs://hfflink/flink/checkpoints/xue_learning_record"
        env.setStateBackend(new RocksDBStateBackend(checkpoint, true))

        val inputStream = env.addSource(new FlinkLogConsumer[RawLogGroupList](deserializer, configProps))

        val stream = new DataStream[RawLogGroupList](inputStream)  //转化成Scala的DataStream
        val result = transform(stream)

        result.addSink(new SourceSink[Record](tool, "aliyun_xue_learning_record").elasticSearchSink())

        env.execute("aliyun_xue_learning_record")

    }

    def transform(stream: DataStream[RawLogGroupList]): DataStream[Record] = {
        val rawLogGroup: DataStream[RawLogGroup] = stream.flatMap(r => {
            val list = r.rawLogGroups.asScala
            list.iterator
        })

        val rawLog: DataStream[RawLog] = rawLogGroup.flatMap(r => {
            val list = r.getLogs.asScala
            list.iterator
        })

        val contents = rawLog.map(_.contents).filter(_.get("level") == "INFO")

        val result = contents.map(c => {
            try {
                //- 5622748_505389_0 {"a":"m","t":"control","controlType":"scrollTop","top":478.5,"quizId":"35972473","s":"505389","tm":1544510196118,"i":24337}, class=com.hfjy.learning.record}}}
                val line = c.get("message").substring(1).trim
                val arr = line.split("(\t| )\\{")
                val userInfo = arr(0).trim
                val command = "{" + arr(1).trim


                val dateTime = c.get("time").substring(0, 23)
                println(dateTime)
                val user = userInfo.split("_")
                val lessonPlanId = user(0).toInt
                val userId = user(1).toInt
                val userType = user(2).toInt
                val index = JSON.parseObject(command).getIntValue("i")
                Record(dateTime, lessonPlanId, userId, userType, index, command)
            } catch {
                case e: Exception => Record()
            }

        }).filter(_.dateTime != "")
        result
    }

    def checkArguments(tool: ParameterTool): Boolean = {
        tool.has("output")
    }
}
