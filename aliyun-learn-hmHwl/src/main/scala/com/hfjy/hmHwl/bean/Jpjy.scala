package com.hfjy.hmHwl.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-10-9. 
  */
case class Jpjy(@BeanProperty override val dateTime: String = "",
                @BeanProperty var jpjyType: String = "-1",
                @BeanProperty var paperId: String = "-1",
                @BeanProperty var quizId: String = "-1",
                @BeanProperty var lessonPlanId: String = "-1",
                @BeanProperty var homeWorkId: String = "-1"
               ) extends Time
