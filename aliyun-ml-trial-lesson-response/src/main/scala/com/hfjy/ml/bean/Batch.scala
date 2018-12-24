package com.hfjy.ml.bean

import java.util

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-12-14. 
  */
case class Batch(@BeanProperty var batch: Int = 0,
                 @BeanProperty var teacher_ids: java.util.List[Int] = new util.ArrayList[Int](),
                 @BeanProperty var time_interval: Int = 0
                )
