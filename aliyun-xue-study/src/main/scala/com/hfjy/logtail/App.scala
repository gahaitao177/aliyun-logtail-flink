package com.hfjy.logtail

import java.util.Properties

import com.aliyun.openservices.log.flink.{ConfigConstants, FlinkLogConsumer}
import com.aliyun.openservices.log.flink.data.{RawLog, RawLogGroup, RawLogGroupList, RawLogGroupListDeserializer}
import com.aliyun.openservices.log.flink.util.Consts
import com.hfjy.logtail.bean.Content
import com.hfjy.logtail.flink.SourceSink
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._
import scala.collection.JavaConverters._

/**
  * Created by kehailin on 2018-11-27.
  *
  * Project: study
  * Logstore: xue-gateway-learn
  * 对应离线数据： study
  */
object App {
    def main(args: Array[String]): Unit = {

        val tool: ParameterTool = ParameterTool.fromArgs(args)
        if (!checkArguments(tool)){
            System.err.println("No port specified. Please run 'App --output <output>'")
            System.exit(1)
        }

        val (configProps, deserializer) = getConfig

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.enableCheckpointing(10000)

        val inputStream = env.addSource(new FlinkLogConsumer[RawLogGroupList](deserializer, configProps))

        val stream = new DataStream[RawLogGroupList](inputStream)  //转化成Scala的DataStream
        val result = transform(stream)

        result.addSink(new SourceSink[Content](tool).elasticSearchSink())
        env.execute("aliyun_xue_study")
    }


    private def getConfig = {
        val configProps = new Properties()
        // 设置访问日志服务的域名
        configProps.put(ConfigConstants.LOG_ENDPOINT, "cn-hangzhou.log.aliyuncs.com")
        // 设置访问ak
        configProps.put(ConfigConstants.LOG_ACCESSSKEYID, "LTAIJ9uvDTqFd5ey")
        configProps.put(ConfigConstants.LOG_ACCESSKEY, "LKGQfDOA08FjNtVMPe3FmuMuIATTRT")

        // 设置日志服务的consumer_group
        configProps.put(ConfigConstants.LOG_CONSUMERGROUP, "bigdata-streaming-study")

        // 设置日志服务的project
        configProps.put(ConfigConstants.LOG_PROJECT, "study")
        // 设置日志服务的Logstore
        configProps.put(ConfigConstants.LOG_LOGSTORE, "study_learn")
        // 设置消费日志服务起始位置
        configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_END_CURSOR)
        // 设置日志服务的消息反序列化方法
        val deserializer = new RawLogGroupListDeserializer()
        (configProps, deserializer)
    }

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

    def checkArguments(tool: ParameterTool): Boolean = {
        tool.has("output")
    }
}
