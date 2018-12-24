package com.hfjy.ml.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-12-17. 
  */
case class BatchModelATeacherId(@BeanProperty override val dateTime: String = "",
                                @BeanProperty var order_id: Int = -1,
                                @BeanProperty var student_id: Int = -1,
                                @BeanProperty var y_proba_model1: Double = 0.0,
                                @BeanProperty var y_proba_model2: Double = 0.0,
                                @BeanProperty var teacher_id: Int = -1,
                                @BeanProperty var batch: Int = -1,
                                @BeanProperty var teacher_job_type: Int = -1,
                                @BeanProperty var filled_new_rate: Int = -1,
                                @BeanProperty var filled_new_student_city_class: Int = -1,
                                @BeanProperty var filled_new_teacher_city_class: Int = -1,
                                @BeanProperty var y_proba_xgb: Double = 0.0,
                                @BeanProperty var y_proba_gbdt: Double = 0.0,
                                @BeanProperty var y_proba_rf: Double = 0.0,
                                @BeanProperty var score_minmax: Double = 0.0,
                                @BeanProperty var final_score: Double = 0.0,
                                @BeanProperty var three_score_minmax: Double = 0.0,
                                @BeanProperty var three_final_score: Double = 0.0,
                                @BeanProperty var trial_lesson_count: Int = -1,
                                @BeanProperty var trial_success_rate: Double = 0.0,
                                @BeanProperty var refined_trial_success_rate: Double = 0.0,
                                @BeanProperty var version: String = "",
                                @BeanProperty var code: Int = 0,
                                @BeanProperty var message: String = ""
                               ) extends Time