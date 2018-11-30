package com.hfjy.logtail.bean

import scala.beans.BeanProperty

/**
  * Created by kehailin on 2018-11-14. 
  */
case class Contents(@BeanProperty override val dateTime: String = "-1",
                    @BeanProperty remote_addr: String = "-1",
                    @BeanProperty upstream_addr: String = "",
                    @BeanProperty uid_set: String = "",
                    @BeanProperty body_bytes_sent: Int = 0,
                    @BeanProperty http_uuid: String = "",
                    @BeanProperty request_method: String = "",
                    @BeanProperty uri: String = "",
                    @BeanProperty http_host: String = "",
                    @BeanProperty uid_got: String = "",
                    @BeanProperty http_user_agent: String = "",
                    @BeanProperty time_iso8601: String = "",
                    @BeanProperty args: String = "",
                    @BeanProperty arg_roomId: String = "",
                    @BeanProperty request_time: Double = .0,
                    @BeanProperty http_referer: String = "",
                    @BeanProperty upstream_name: String = "",
                    @BeanProperty http_x_hf_learn_session_id: String = "",
                    @BeanProperty http_x_forwarded_for: String = "",
                    @BeanProperty upstream_response_time: String = "",
                    @BeanProperty http_cookie: String = "",
                    @BeanProperty status: Int = -1,
                    @BeanProperty server_protocol: String = ""
                   ) extends Time
