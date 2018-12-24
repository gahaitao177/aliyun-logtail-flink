package com.hfjy.ml.bean

import java.util

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-12-14. 
  */
case class LessonData(@BeanProperty var lesson_begin_time: String = "",
                      @BeanProperty var notice_type: Int = 0,
                      @BeanProperty var order_id: Int = 0,
                      @BeanProperty var teacher_id: java.util.List[Int] = new util.ArrayList[Int]()
                     )
