package com.hfjy.ml.util

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

        // 设置日志服务的consumer_group
        configProps.put(ConfigConstants.LOG_CONSUMERGROUP, "bigdata-streaming-ml-trial-lesson-request")
        // 设置日志服务的project
        configProps.put(ConfigConstants.LOG_PROJECT, "trial-lesson-assigning")
        // 设置日志服务的Logstore
        configProps.put(ConfigConstants.LOG_LOGSTORE, "trial_lesson_request")
        // 设置消费日志服务起始位置
        configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_END_CURSOR)
        // 设置日志服务的消息反序列化方法
        val deserializer = new RawLogGroupListDeserializer()
        (configProps, deserializer)
    }
}
