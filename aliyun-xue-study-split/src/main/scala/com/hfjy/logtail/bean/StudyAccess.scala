package com.hfjy.logtail.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-11-27. 
  */
case class StudyAccess(@BeanProperty override val dateTime: String = "",
                       @BeanProperty var httpStatus: String = "",
                       @BeanProperty var ip: String = "",
                       @BeanProperty var action: String = "",
                       @BeanProperty var method: String = "",
                       @BeanProperty var uri: String = "",
                       @BeanProperty var params: String = "",
                       @BeanProperty var referer: String = "",
                       @BeanProperty var ua: String = "",
                       @BeanProperty var uuid: String = "",
                       @BeanProperty var headers: String = "",
                       @BeanProperty var responseSize: String = "",
                       @BeanProperty var takeTime: String = "",
                       @BeanProperty var sessionDevice: String = ""
                      ) extends Time
