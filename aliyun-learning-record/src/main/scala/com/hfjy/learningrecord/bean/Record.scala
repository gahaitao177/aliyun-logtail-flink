package com.hfjy.learningrecord.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-12-6. 
  */
case class Record(@BeanProperty override val dateTime: String = "",
                  @BeanProperty var lessonPlanId: Int = 0,
                  @BeanProperty var userId: Int = 0,
                  @BeanProperty var userType: Int = -1,
                  @BeanProperty var index: Int = 0,
                  @BeanProperty var command: String = "") extends Time
