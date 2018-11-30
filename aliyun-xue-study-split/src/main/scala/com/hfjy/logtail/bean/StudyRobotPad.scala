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
                         @BeanProperty var deviceNodeNo: String = "",
                         @BeanProperty var orderVersion: String = "",
                         @BeanProperty var deviceSdkVersion: String = ""
                        ) extends Time
