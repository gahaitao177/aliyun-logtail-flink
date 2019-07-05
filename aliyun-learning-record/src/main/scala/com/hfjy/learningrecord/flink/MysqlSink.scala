package com.hfjy.learningrecord.flink

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Date

import com.hfjy.learningrecord.bean.LessonQuiz
import com.hfjy.learningrecord.util.ConfigUtil
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.createTypeInformation

import scala.collection.mutable.ListBuffer

/**
  * Created by kehailin on 2019-3-6. 
  */
class MysqlSink extends RichSinkFunction[LessonQuiz] with CheckpointedFunction{

    @transient
    private var checkpointedState: ListState[LessonQuiz] = _
    private val bufferedElements = ListBuffer[LessonQuiz]()

    val dbConf = ConfigUtil.getDBConfig()

    val dburl = dbConf.getProperty("dburl")
    val username = dbConf.getProperty("username")
    val password = dbConf.getProperty("password")
    val tablename = dbConf.getProperty("tablename")

    var conn: Connection = _
    var preparedStatement: PreparedStatement = _

    val threshold = 10

    override def open(parameters: Configuration) {
        println(new Date() + "-----------open------------")
        super.open(parameters)
        Class.forName("com.mysql.jdbc.Driver")
        conn = DriverManager.getConnection(dburl, username, password)
        conn.setAutoCommit(false)
    }

    override def close() {
        println(new Date() + "------------close-------------")
        super.close()
        if (conn != null) {
            conn.close()
        }
    }

    override def invoke(value: LessonQuiz) {
        bufferedElements += value

        if (bufferedElements.size == threshold) {
            val sql = s"replace into $tablename (date_time, lesson_plan_id, direct, quiz_id) values(?,?,?,?)"
            preparedStatement = conn.prepareStatement(sql)
            try {
                for (element <- bufferedElements) {
                    preparedStatement.setString(1, element.dateTime)
                    preparedStatement.setInt(2, element.lessonPlanId)
                    preparedStatement.setInt(3, element.direct)
                    preparedStatement.setString(4, element.quizId)
                    preparedStatement.addBatch()
                }
                val result = preparedStatement.executeBatch()
                conn.commit()
                preparedStatement.clearBatch()
                bufferedElements.clear()
            } catch {
                case e: Exception =>
                    bufferedElements.clear()
            }
        }
    }

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
        checkpointedState.clear()
        for (element <- bufferedElements) {
            checkpointedState.add(element)
        }
    }

    override def initializeState(context: FunctionInitializationContext): Unit = {

        val descriptor = new ListStateDescriptor[LessonQuiz]("content-sink", createTypeInformation[LessonQuiz])
        checkpointedState = context.getOperatorStateStore.getListState(descriptor)

        if (context.isRestored) {
            for(element <- checkpointedState.get()) {
                bufferedElements += element
            }
        }

    }
}
