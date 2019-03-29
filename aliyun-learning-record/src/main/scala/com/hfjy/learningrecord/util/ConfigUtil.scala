package com.hfjy.learningrecord.util

import java.util.Properties

import com.aliyun.openservices.log.flink.ConfigConstants
import com.aliyun.openservices.log.flink.data.RawLogGroupListDeserializer
import com.aliyun.openservices.log.flink.util.Consts

/**
  * Created by kehailin on 2018-11-27. 
  */
object ConfigUtil {

    def getConfig = {
        val configProps = new Properties()
        // 设置访问日志服务的域名
        configProps.put(ConfigConstants.LOG_ENDPOINT, "cn-hangzhou.log.aliyuncs.com")
        // 设置访问ak
        configProps.put(ConfigConstants.LOG_ACCESSSKEYID, "LTAIJ9uvDTqFd5ey")
        configProps.put(ConfigConstants.LOG_ACCESSKEY, "LKGQfDOA08FjNtVMPe3FmuMuIATTRT")

        // 用于获取Logstore中所有的shard列表，获取shard状态等.如果您的shard经常发生分裂合并，可以通过调整接口的调用周期来及时发现shard的变化
        // 设置每30s调用一次ListShards
        configProps.put(ConfigConstants.LOG_SHARDS_DISCOVERY_INTERVAL_MILLIS, "30000")
        // 设置日志服务的consumer_group
        configProps.put(ConfigConstants.LOG_CONSUMERGROUP, "bigdata-streaming-learning-record")
        // 设置日志服务的project
        configProps.put(ConfigConstants.LOG_PROJECT, "learning")
        // 设置日志服务的Logstore
        configProps.put(ConfigConstants.LOG_LOGSTORE, "classroom1")
        // 设置消费日志服务起始位置
        configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_END_CURSOR)
//        configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, "1546232950")  //2018-12-31 13:09:10
        // 设置日志服务的消息反序列化方法
        val deserializer = new RawLogGroupListDeserializer()
        (configProps, deserializer)
    }

    def getDBConfig() = {
        val inputStream = ConfigUtil.getClass.getClassLoader.getResourceAsStream("db.properties")
        val conf = new Properties()
        conf.load(inputStream)
        conf
    }
}
