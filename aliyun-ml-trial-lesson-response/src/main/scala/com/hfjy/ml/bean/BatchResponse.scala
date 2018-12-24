package com.hfjy.ml.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-12-14. 
  */
case class BatchResponse(@BeanProperty override val dateTime: String = "",
                         @BeanProperty var orderId: Int = 0,
                         @BeanProperty var code: Int = 0,
                         @BeanProperty var batch: Int = 0,
                         @BeanProperty var teacherId: Int = 0,
                         @BeanProperty var timeInterval: Int = 0,
                         @BeanProperty var noticeType: Int = 0,
                         @BeanProperty var message: String = ""
                        ) extends Time
