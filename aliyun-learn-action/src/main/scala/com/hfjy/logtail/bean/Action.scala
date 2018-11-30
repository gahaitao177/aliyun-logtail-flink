package com.hfjy.logtail.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-11-30. 
  */
case class Action(@BeanProperty override val dateTime: String = "",
                  @BeanProperty var funtion: String = "",
                  @BeanProperty var userId: String = "",
                  @BeanProperty var userType: String = "",
                  @BeanProperty var name: String = "",
                  @BeanProperty var lessonIdName: String = "",
                  @BeanProperty var lessonId: String = ""
                   ) extends Time
