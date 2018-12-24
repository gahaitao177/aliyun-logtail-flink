package com.hfjy.ml.bean

import java.util

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-12-14. 
  */
case class BatchData(@BeanProperty var batches: java.util.List[Batch] = new util.ArrayList[Batch](),
                     @BeanProperty var notice_type: Int = 0,
                     @BeanProperty var order_id: Int = 0
               )
