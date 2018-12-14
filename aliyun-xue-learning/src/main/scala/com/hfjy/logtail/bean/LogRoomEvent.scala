package com.hfjy.logtail.bean

/**
  * Created by kehailin on 2018-12-5.
  *
  * 2018-12-10 15:55:17.053272 classroom.go:562 INFO - [RoomEvent]  {"event":"RoomEvent","lessonPlanId":"3612228","currentTimeStamp":"2018-12-10 15:55:17.052","clientId":"3612228_26851_1","lessonState":"授课中","lessonPlanName":"高三地理专项课程—F","eventName":"进入","osName":"win10Pc","clientName":"xueApp","clientVersion":"3"}
  */
case class LogRoomEvent(event: String,
                        lessonPlanId: String,
                        currentTimeStamp: String,
                        clientId: String,
                        lessonState: String,
                        lessonPlanName: String,
                        eventName: String,
                        osName: String,
                        clientName: String,
                        clientVersion: String)
