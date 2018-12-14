package com.hfjy.logtail.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-12-10.
  * 2018-12-06 09:00:56,227 com.hfjy.log INFO - [user-use-videoBypass]	{"abtestVersion:":1,"student_id:":43731,"lesson_plan_id:":6164492}
  */
case class Video(@BeanProperty override val dateTime: String = "",
                 @BeanProperty var abtestVersion: Int = -1,
                 @BeanProperty var student_id: Int = 0,
                 @BeanProperty var lesson_plan_id: Int = 0) extends Time
