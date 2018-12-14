package com.hfjy.ml.bean

import java.util

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-12-13. 
  */
case class LessonRequest(
                         @BeanProperty var figures: java.util.List[Figure] = new util.ArrayList[Figure](),
                         @BeanProperty var count: Int = 0
                        )