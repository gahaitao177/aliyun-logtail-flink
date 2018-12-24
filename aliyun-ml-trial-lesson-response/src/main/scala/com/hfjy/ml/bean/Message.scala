package com.hfjy.ml.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-12-14. 
  */
case class Message(@BeanProperty var code: Int = 0,
                   @BeanProperty var message: String = "",
                   @BeanProperty var data: String = ""
                  )
