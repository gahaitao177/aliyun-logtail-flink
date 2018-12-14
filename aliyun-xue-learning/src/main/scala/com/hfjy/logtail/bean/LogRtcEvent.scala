package com.hfjy.logtail.bean

/**
  * Created by kehailin on 2018-12-5.
  *
  * 2018-12-10 16:09:58.283676 classroom.go:526 INFO - [RtcEvent]    {"event":"RtcEvent","lessonPlanId":"3612228","currentTimeStamp":"2018-12-10 16:09:58.282","userId":"26851","userName":"测试常","userType":"1","desc":"切换","source":"artc","status":"1","label":"音频一","postfix":"1544429398"}
  */
case class LogRtcEvent(event: String,
                       lessonPlanId: String,
                       currentTimeStamp: String,
                       userId: String,
                       userName: String,
                       userType: String,
                       desc: String,
                       source: String,
                       status: String,
                       label: String,
                       postfix: String)
