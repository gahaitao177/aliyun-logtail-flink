package com.hfjy.learningrecord.bean

import scala.beans.BeanProperty


/**
  * Created by kehailin on 2018-11-9. 
  */
trait Time {
    @BeanProperty val dateTime: String = "-1"
}
