package com.hfjy.ml.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-12-17. 
  */
case class AssigningModelTeacherId(@BeanProperty override val dateTime: String = "",
                                   @BeanProperty var student_id: Int = -1,
                                   @BeanProperty var order_id: Int = -1,
                                   @BeanProperty var teacher_id: Int = -1,
                                   @BeanProperty var province_byphone: Int = -1,
                                   @BeanProperty var new_studentSource: Int = -1,
                                   @BeanProperty var firstTrialCourse: Int = -1,
                                   @BeanProperty var seniorSchoolInfo: Int = -1,
                                   @BeanProperty var new_studentGrade: Int = -1,
                                   @BeanProperty var grade_subject: Int = -1,
                                   @BeanProperty var new_classRank: Int = -1,
                                   @BeanProperty var new_new_studentSex: Int = -1,
                                   @BeanProperty var teacherSex: Double = 0.0,
                                   @BeanProperty var new_student_City_Class: Int = -1,
                                   @BeanProperty var new_teacherPlanCourseInterval: Int = -1,
                                   @BeanProperty var teachedTrialCourseCount: Int = -1,
                                   @BeanProperty var effectiveCommunicationCount: Int = -1,
                                   @BeanProperty var score_mean: Double = 0.0,
                                   @BeanProperty var score_min: Double = 0.0,
                                   @BeanProperty var new_learningTarget: Int = -1,
                                   @BeanProperty var teacher_audition_success_rate: Int = -1,
                                   @BeanProperty var new_new_teached_age: Double = 0.0,
                                   @BeanProperty var new_teacher_daily_audition_count: Double = 0.0,
                                   @BeanProperty var new_teacher_daily_audition_success_count: Int = -1,
                                   @BeanProperty var new_new_selfEvaluation: Double = 0.0,
                                   @BeanProperty var new_new_rate: Double = 0.0,
                                   @BeanProperty var new_coil_in: Int = -1,
                                   @BeanProperty var lm_informal_teached_lesson_count: Int = -1,
                                   @BeanProperty var taught_total_time: Int = -1,
                                   @BeanProperty var first_tkod_tifl_count: Int = -1,
                                   @BeanProperty var teachedTrialSuccessStudentCount: Int = -1,
                                   @BeanProperty var rf_prob: Double = 0.0,
                                   @BeanProperty var gbdt_prob: Double = 0.0,
                                   @BeanProperty var xgb_prob: Double = 0.0,
                                   @BeanProperty var lgb_prob: Double = 0.0,
                                   @BeanProperty var catboost_prob: Double = 0.0
                                  ) extends Time
