package com.hfjy.ml.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-12-13. 
  */
case class Figure(@BeanProperty var earliest_lesson_begin_time: String = "",
                  @BeanProperty var earliest_lesson_end_time: String = "",
                  @BeanProperty var student_id: Int = 0,
                  @BeanProperty var teacher_ids: java.util.List[Int] = new java.util.ArrayList[Int],
                  @BeanProperty var order_id: String = ""
                 )
