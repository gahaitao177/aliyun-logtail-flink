package com.hfjy.logtail.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-11-27. 
  */
case class Content(@BeanProperty override val dateTime: String = "",
                   @BeanProperty var logmessage: String = "") extends Time