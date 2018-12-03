package com.hfjy.logtail.flink

import com.hfjy.logtail.bean.Time
import com.hfjy.logtail.impl.RestClientFactoryImpl
import com.hfjy.logtail.util.BeanUtil
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Created by kehailin on 2018-8-23.
  *
  * SourceSink类的参数 topic 没有序列化会报错
  */
class SourceSink[T <: Time](tool: ParameterTool, topic: String) extends Serializable {

    def elasticSearchSink(): ElasticsearchSink[T] = {

        val httpHosts = new java.util.ArrayList[HttpHost]()
        val elastics: String = tool.get("output")
        val elas = elastics.split(",")
        for (i <- 0 until elas.size){
            val es = elas(i).split(":")
            val host = es(0)
            val port = es(1).toInt
            httpHosts.add(new HttpHost(host, port, "http"))
        }

        //output
        val esSinkBuilder = new ElasticsearchSink.Builder[T](
            httpHosts,
            new ElasticsearchSinkFunction[T] {
                override def process(t: T, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
                    requestIndexer.add(createIndexRequest(t))
                }

                def createIndexRequest(element: T): IndexRequest = {
                    val dt = element.dateTime.substring(0, 10).replace("-","")
                    val json = BeanUtil.bean2Map(element)
                    val timestamp = new DateTime(json.get("date_time").toString.replace(" ", "T"), DateTimeZone.UTC).toDateTime()
                    json.put("timestamp", timestamp)

                    Requests.indexRequest()
                        .index(topic + "_" + dt)
                        .`type`(topic + "_type")
                        .source(json)
                }
            }
        )

        esSinkBuilder.setBulkFlushMaxActions(10)
        esSinkBuilder.setRestClientFactory(new RestClientFactoryImpl)
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler())

        esSinkBuilder.build()
    }

}

