package com.hfjy.logtail.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-11-29. 
  */
case class Message(@BeanProperty override val dateTime: String = "",
                   @BeanProperty var fromPageUrl: String = "",
                   @BeanProperty var fromPageName: String = "",
                   @BeanProperty var openUniCode: String = "",
                   @BeanProperty var remark: String = "",
                   @BeanProperty var httpUserAgent: String = "",
                   @BeanProperty var userId: Int = 0,
                   @BeanProperty var clientTime: Long = 0L,
                   @BeanProperty var toPageName: String = "",
                   @BeanProperty var actionId: String = "",
                   @BeanProperty var action: String = "",
                   @BeanProperty var serverTime: String = "",
                   @BeanProperty var event: String = "",
                   @BeanProperty var toPageUrl: String = ""
                  ) extends Time