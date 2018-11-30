package com.hfjy.logtail.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-8-27. 
  */
case class Business(@BeanProperty var logType: String = "",
                    @BeanProperty var originDateTime: String = "",
                    @BeanProperty override val dateTime: String = "",
                    @BeanProperty var lessonPlanId: Int = 0,
                    @BeanProperty var courseStatus: String = "",
                    @BeanProperty var courseName: String = "",
                    @BeanProperty var userId: Int = 0,
                    @BeanProperty var userName: String  = "",
                    @BeanProperty var userType: Int = -1,
                    @BeanProperty var action: String = "",
                    @BeanProperty var currentRtc: String = "",
                    @BeanProperty var voiceSetStr: String = "",
                    @BeanProperty var videoSetStr: String = "",
                    @BeanProperty var channel: String = "",
                    @BeanProperty var channelType: Int = -1,
                    @BeanProperty var userAgent: String = "",
                    @BeanProperty var deviceId: String = "",
                    @BeanProperty var messageSize: Int = 0) extends Time
