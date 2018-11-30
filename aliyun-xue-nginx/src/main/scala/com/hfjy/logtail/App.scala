package com.hfjy.logtail

import com.aliyun.openservices.log.flink.data.{RawLog, RawLogGroup, RawLogGroupList}
import com.aliyun.openservices.log.flink.FlinkLogConsumer
import com.hfjy.logtail.bean.Contents
import com.hfjy.logtail.flink.SourceSink
import com.hfjy.logtail.util.ConfigUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import scala.collection.JavaConverters._

/**
  * Created by kehailin on 2018-11-13.
  *          序号:                        1                 2                               3
  *  LOG_PROJECT:                    study        study-nginx                           study
  * LOG_LOGSTORE:                classroom              nginx   xue-gateway-learning_learning
  * 日志中的class: com.hfjy.learning.record          xue_nginx                        learning
  *
  * Project: study-nginx
  * Logstore: nginx
  * 对应离线数据： xue nginx access
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
        val checkpoint = "hdfs://hfflink/flink/checkpoints/xue_nginx"
        env.setStateBackend(new RocksDBStateBackend(checkpoint, true))

        val inputStream = env.addSource(new FlinkLogConsumer[RawLogGroupList](deserializer, configProps))

        val stream = new DataStream[RawLogGroupList](inputStream)  //转化成Scala的DataStream
        val result = transform(stream)

        result.addSink(new SourceSink[Contents](tool, "aliyun_xue_nginx").elasticSearchSink())

        env.execute("aliyun_xue_nginx")
    }

    def transform(stream: DataStream[RawLogGroupList]): DataStream[Contents] = {
        val rawLogGroup: DataStream[RawLogGroup] = stream.flatMap(r => {
            val list = r.rawLogGroups.asScala
            list.iterator
        })

        val rawLog: DataStream[RawLog] = rawLogGroup.flatMap(r => {
            val list = r.getLogs.asScala
            list.iterator
        })

        val result = rawLog.map(r => {
            try {
                val content = r.getContents

                val remote_addr = content.getOrDefault("remote_addr", "")
                val upstream_addr = content.getOrDefault("upstream_addr", "")
                val uid_set = content.getOrDefault("uid_set", "")
                val body_bytes_sent = content.getOrDefault("body_bytes_sent", "").toInt
                val http_uuid = content.getOrDefault("http_uuid", "")
                val request_method = content.getOrDefault("request_method", "")
                val uri = content.getOrDefault("uri", "")
                val http_host = content.getOrDefault("http_host", "")
                val uid_got = content.getOrDefault("uid_got", "")
                val http_user_agent = content.getOrDefault("http_user_agent", "")
                val time_iso8601 = content.getOrDefault("time_iso8601", "")
                val date_time = time_iso8601.substring(0, 19).replace("T", " ")
                val args = content.getOrDefault("args", "")
                val arg_roomId = content.getOrDefault("arg_roomId", "")
                val request_time = content.getOrDefault("request_time", "").toDouble
                val http_referer = content.getOrDefault("http_referer", "")
                val upstream_name = content.getOrDefault("upstream_name", "")
                val http_x_hf_learn_session_id = content.getOrDefault("http_x_hf_learn_session_id", "")
                val http_x_forwarded_for = content.getOrDefault("http_x_forwarded_for", "")
                val upstream_response_time = content.getOrDefault("upstream_response_time", "")
                val http_cookie = content.getOrDefault("http_cookie", "")
                val status = content.getOrDefault("status", "").toInt
                val server_protocol = content.getOrDefault("server_protocol", "")
                Contents(date_time, remote_addr, upstream_addr, uid_set, body_bytes_sent,
                    http_uuid, request_method, uri, http_host, uid_got, http_user_agent,
                    time_iso8601, args, arg_roomId, request_time, http_referer, upstream_name,
                    http_x_hf_learn_session_id, http_x_forwarded_for, upstream_response_time,
                    http_cookie, status, server_protocol
                )
            } catch {
                case e: Exception => Contents()
            }
        }).filter(_.dateTime != "-1")
        result
    }

    def checkArguments(tool: ParameterTool): Boolean = {
        tool.has("output")
    }
}
