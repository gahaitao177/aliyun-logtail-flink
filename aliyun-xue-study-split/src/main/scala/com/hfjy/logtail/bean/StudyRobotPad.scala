package com.hfjy.logtail.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-11-27. 
  */
case class StudyRobotPad(@BeanProperty override val dateTime: String = "",
                         @BeanProperty var lessonPlanId: Int = 0,
                         @BeanProperty var userId: Int = 0,
                         @BeanProperty var userType: Int = 0,
                         @BeanProperty var deviceVersion: String = "",
                         @BeanProperty var deviceModeNo: String = "",
                         @BeanProperty var orderVersion: String = "",
                         @BeanProperty var deviceSdkVersion: String = "",
                         @BeanProperty var sourceId: String = "",
                         @BeanProperty var source: String = "",
                         @BeanProperty var connectStatus: String = "",
                         @BeanProperty var appName: String = ""
                        ) extends Time
