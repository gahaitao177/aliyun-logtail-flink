package com.hfjy.logtail.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-12-5. 
  */
case class ClassRoom(@BeanProperty override val dateTime: String = "",
                     @BeanProperty var logType: String = "",
                     @BeanProperty var lessonPlanId: Int = 0,
                     @BeanProperty var userId: Int = 0,
                     @BeanProperty var userType: Int = -1,
                     @BeanProperty var userName: String = "",
                     @BeanProperty var action: String = "",
                     @BeanProperty var currentRtc: String = "",
                     @BeanProperty var channel: String = "",
                     @BeanProperty var channelType: String = "",
                     @BeanProperty var courseStatus: String = "",
                     @BeanProperty var courseName: String = "",
                     @BeanProperty var userAgent: String = ""
                    ) extends Time
