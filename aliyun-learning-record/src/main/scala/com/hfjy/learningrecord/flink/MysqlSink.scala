package com.hfjy.learningrecord.flink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.hfjy.learningrecord.bean.LessonQuiz
import com.hfjy.learningrecord.util.ConfigUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * Created by kehailin on 2019-3-6. 
  */
class MysqlSink extends RichSinkFunction[LessonQuiz] {

    val dbConf = ConfigUtil.getDBConfig()

    val dburl = dbConf.getProperty("dburl")
    val username = dbConf.getProperty("username")
    val password = dbConf.getProperty("password")
    val tablename = dbConf.getProperty("tablename")

    var conn: Connection = _
    var preparedStatement: PreparedStatement = _

    override def open(parameters: Configuration) {
        super.open(parameters)
        Class.forName("com.mysql.jdbc.Driver")
        conn = DriverManager.getConnection(dburl, username, password)
        conn.setAutoCommit(false)
    }

    override def close() {
        super.close()
        if (conn != null) {
            conn.close()
        }
    }

    override def invoke(value: LessonQuiz) {
        val sql = s"replace into $tablename (date_time, lesson_plan_id, direct, quiz_id) values(?,?,?,?)"
        preparedStatement = conn.prepareStatement(sql)
        preparedStatement.setString(1, value.dateTime)
        preparedStatement.setInt(2, value.lessonPlanId)
        preparedStatement.setInt(3, value.direct)
        preparedStatement.setString(4, value.quizId)
        val result = preparedStatement.executeUpdate()
        conn.commit()

    }
}
