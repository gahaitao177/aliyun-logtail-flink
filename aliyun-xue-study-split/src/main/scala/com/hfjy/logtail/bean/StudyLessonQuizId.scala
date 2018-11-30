package com.hfjy.logtail.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-11-27. 
  */
case class StudyLessonQuizId(@BeanProperty override val dateTime: String = "",
                             @BeanProperty var lessonPlanId: String = "",
                             @BeanProperty var quizId: String = ""
                            ) extends Time
