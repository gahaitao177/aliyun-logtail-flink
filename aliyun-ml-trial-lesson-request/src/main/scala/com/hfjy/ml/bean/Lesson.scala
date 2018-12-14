package com.hfjy.ml.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-12-13. 
  */
case class Lesson(@BeanProperty override val dateTime: String = "",
                  @BeanProperty var orderId: String = "",
                  @BeanProperty var studentId: Int = 0,
                  @BeanProperty var teacherId: Int = 0,
                  @BeanProperty var count: Int = 0,
                  @BeanProperty var earliestLessonBeginTime: String = "",
                  @BeanProperty var earliestLessonEndTime: String = ""
                 ) extends Time