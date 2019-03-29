add jar /home/bigdata/kehailin/elasticsearch-hadoop-6.4.2.jar;

--1.learning classroom
CREATE EXTERNAL TABLE tmp.es_learning_classroom(
log_type string,
date_time string,
lesson_plan_id bigint,
course_status string,
course_name string,
user_id bigint,
user_name string,
user_type bigint,
action string,
current_rtc string,
channel string,
channel_type string,
user_agent string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_xue_learning_20181219/aliyun_xue_learning_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true'
);

CREATE EXTERNAL TABLE ods_es.es_learning_classroom(
log_type string,
date_time string,
lesson_plan_id bigint,
course_status string,
course_name string,
user_id bigint,
user_name string,
user_type bigint,
action string,
current_rtc string,
channel string,
channel_type string,
user_agent string
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_learning_classroom set tblproperties('es.resource' = 'aliyun_xue_learning_20181218/aliyun_xue_learning_type');

insert overwrite table ods_es.es_learning_classroom partition(dt='20181218')
select * from tmp.es_learning_classroom;


--2.learning record
CREATE EXTERNAL TABLE tmp.es_learning_record(
date_time string,
lesson_plan_id bigint,
user_id bigint,
user_type bigint,
index bigint,
command string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_xue_learning_record_20181219/aliyun_xue_learning_record_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true'
);

CREATE EXTERNAL TABLE ods_es.es_learning_record(
date_time string,
lesson_plan_id bigint,
user_id bigint,
user_type bigint,
index bigint,
command string
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_learning_record set tblproperties('es.resource' = 'aliyun_xue_learning_record_20181213/aliyun_xue_learning_record_type');

insert overwrite table ods_es.es_learning_record partition(dt='20181213')
select * from tmp.es_learning_record;

--3.user action
CREATE EXTERNAL TABLE tmp.es_user_action(
date_time string,
action_id string,
action string,
user_id bigint,
open_uni_code string,
event string,
remark string,
client_time bigint,
server_time string,
http_user_agent string,
from_page_name string,
to_page_name string,
from_page_url string,
to_page_url string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_user_action_20181219/aliyun_user_action_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true',
'es.mapping.date.rich' = 'false'
);

CREATE EXTERNAL TABLE ods_es.es_user_action(
date_time string,
action_id string,
action string,
user_id bigint,
open_uni_code string,
event string,
remark string,
client_time bigint,
server_time string,
http_user_agent string,
from_page_name string,
to_page_name string,
from_page_url string,
to_page_url string
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_user_action set tblproperties('es.resource' = 'aliyun_user_action_20181213/aliyun_user_action_type');

insert overwrite table ods_es.es_user_action partition(dt='20181213')
select * from tmp.es_user_action;


--4.learn hmHwl
CREATE EXTERNAL TABLE tmp.es_learn_hmhwl(
date_time string,
lesson_plan_id string,
jpjy_type string,
paper_id string,
quiz_id string,
home_work_id string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_learn_hmhwl_20181219/aliyun_learn_hmhwl_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true'
);

CREATE EXTERNAL TABLE ods_es.es_learn_hmhwl(
date_time string,
lesson_plan_id string,
jpjy_type string,
paper_id string,
quiz_id string,
home_work_id string
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_learn_hmHwl set tblproperties('es.resource' = 'aliyun_learn_hmhwl_20190328/aliyun_learn_hmhwl_type');

insert overwrite table ods_es.es_learn_hmHwl partition(dt='20181213')
select * from tmp.es_learn_hmHwl;

--5.learn action
CREATE EXTERNAL TABLE tmp.es_learn_action(
date_time string,
user_id string,
user_type string,
name string,
funtion string,
lesson_id_name string,
lesson_id string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_learn_action_20181219/aliyun_learn_action_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true'
);

CREATE EXTERNAL TABLE ods_es.es_learn_action(
date_time string,
user_id string,
user_type string,
name string,
funtion string,
lesson_id_name string,
lesson_id string
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_learn_action set tblproperties('es.resource' = 'aliyun_learn_action_20181213/aliyun_learn_action_type');

insert overwrite table ods_es.es_learn_action partition(dt='20181213')
select * from tmp.es_learn_action;

--6.trial_lesson_request
CREATE EXTERNAL TABLE tmp.es_ml_trial_lesson_request(
date_time string,
order_id string,
student_id bigint,
teacher_id bigint,
count bigint,
earliest_lesson_begin_time string,
earliest_lesson_end_time string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_ml_trial_lesson_request_20181219/aliyun_ml_trial_lesson_request_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true'
);

CREATE EXTERNAL TABLE ods_es.es_ml_trial_lesson_request(
date_time string,
order_id string,
student_id bigint,
teacher_id bigint,
count bigint,
earliest_lesson_begin_time string,
earliest_lesson_end_time string
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_ml_trial_lesson_request set tblproperties('es.resource' = 'aliyun_ml_trial_lesson_request_20181213/aliyun_ml_trial_lesson_request_type');

insert overwrite table ods_es.es_ml_trial_lesson_request partition(dt='20181213')
select * from tmp.es_ml_trial_lesson_request;

--7.trial_lesson_recommend_model_a
CREATE EXTERNAL TABLE tmp.es_ml_trial_lesson_recommend_model_a(
date_time string,
order_id bigint,
student_id bigint,
teacher_id bigint,
batch bigint,
score_minmax float,
three_score_minmax float,
three_final_score float,
final_score float,
y_proba_gbdt float,
y_proba_rf float,
y_proba_xgb float,
y_proba_model1 float,
y_proba_model2 float,
trial_success_rate float,
refined_trial_success_rate float,
filled_new_student_city_class bigint,
filled_new_teacher_city_class bigint,
filled_new_rate bigint,
teacher_job_type bigint,
trial_lesson_count bigint,
code bigint,
message string,
version string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_ml_trial_lesson_recommend_model_a_20181219/aliyun_ml_trial_lesson_recommend_model_a_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true'
);

CREATE EXTERNAL TABLE ods_es.es_ml_trial_lesson_recommend_model_a(
date_time string,
order_id bigint,
student_id bigint,
teacher_id bigint,
batch bigint,
score_minmax float,
three_score_minmax float,
three_final_score float,
final_score float,
y_proba_gbdt float,
y_proba_rf float,
y_proba_xgb float,
y_proba_model1 float,
y_proba_model2 float,
trial_success_rate float,
refined_trial_success_rate float,
filled_new_student_city_class bigint,
filled_new_teacher_city_class bigint,
filled_new_rate bigint,
teacher_job_type bigint,
trial_lesson_count bigint,
code bigint,
message string,
version string
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_ml_trial_lesson_recommend_model_a set tblproperties('es.resource' = 'aliyun_ml_trial_lesson_recommend_model_a_20181213/aliyun_ml_trial_lesson_recommend_model_a_type');

insert overwrite table ods_es.es_ml_trial_lesson_recommend_model_a partition(dt='20181213')
select * from tmp.es_ml_trial_lesson_recommend_model_a;


--8.trial_lesson_recommend_model_b
CREATE EXTERNAL TABLE tmp.es_ml_trial_lesson_recommend_model_b(
date_time string,
order_id bigint,
student_id bigint,
teacher_id bigint,
batch bigint,
new_student_grade bigint,
lm_informal_teached_lesson_count bigint,
new_teacher_age bigint,
y_proba_rf float,
have_after_plan bigint,
new_new_self_evaluation float,
new_grade_rank bigint,
first_trial_course bigint,
teached_trial_success_student_count bigint,
teached_official_student_count bigint,
teacher_audition_success_rate float,
grade_subject bigint,
y_proba_catboost float,
teacher_job_type bigint,
new_teacher_plan_course_interval float,
cross_sex bigint,
version string,
real_new_rate float,
final_score float,
new_learning_target bigint,
first_tkod_tifl_count float,
new_class_rank bigint,
student_no bigint,
score_mean float,
new_teacher_trial_course_count bigint,
new_coil_in bigint,
subject_id bigint,
new_student_source bigint,
y_proba_gbdt float,
code bigint,
filled_new_class_rank bigint,
new_student_city_class bigint,
effective_communication_count bigint,
teached_trial_course_count bigint,
filled_senior_school_info bigint,
senior_school_info bigint,
filled_new_student_city_class bigint,
province_byphone bigint,
score_min float,
new_teacher_daily_audition_success_count float,
new_college_type bigint,
new_exam_year bigint,
taught_total_time bigint,
filled_new_coil_in bigint,
new_teacher_daily_audition_count float,
refined_trial_success_rate float,
y_proba_xgb float,
trial_success_count bigint,
score_max float,
new_senior_class_info bigint,
filled_new_rate bigint,
minmax_score float,
filled_province_byphone bigint,
message string,
enable_trial_count bigint,
new_new_teached_age float,
filled_new_new_rate bigint,
new_new_student_sex bigint,
new_self_evaluation bigint,
filled_new_student_source bigint,
teacher_sex bigint,
filled_new_teacher_city_class bigint,
new_student_present_province bigint,
new_teacher_city_class bigint
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_ml_trial_lesson_recommend_model_b_20181219/aliyun_ml_trial_lesson_recommend_model_b_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true'
);

CREATE EXTERNAL TABLE ods_es.es_ml_trial_lesson_recommend_model_b(
date_time string,
order_id bigint,
student_id bigint,
teacher_id bigint,
batch bigint,
new_student_grade bigint,
lm_informal_teached_lesson_count bigint,
new_teacher_age bigint,
y_proba_rf float,
have_after_plan bigint,
new_new_self_evaluation float,
new_grade_rank bigint,
first_trial_course bigint,
teached_trial_success_student_count bigint,
teached_official_student_count bigint,
teacher_audition_success_rate float,
grade_subject bigint,
y_proba_catboost float,
teacher_job_type bigint,
new_teacher_plan_course_interval float,
cross_sex bigint,
version string,
real_new_rate float,
final_score float,
new_learning_target bigint,
first_tkod_tifl_count float,
new_class_rank bigint,
student_no bigint,
score_mean float,
new_teacher_trial_course_count bigint,
new_coil_in bigint,
subject_id bigint,
new_student_source bigint,
y_proba_gbdt float,
code bigint,
filled_new_class_rank bigint,
new_student_city_class bigint,
effective_communication_count bigint,
teached_trial_course_count bigint,
filled_senior_school_info bigint,
senior_school_info bigint,
filled_new_student_city_class bigint,
province_byphone bigint,
score_min float,
new_teacher_daily_audition_success_count float,
new_college_type bigint,
new_exam_year bigint,
taught_total_time bigint,
filled_new_coil_in bigint,
new_teacher_daily_audition_count float,
refined_trial_success_rate float,
y_proba_xgb float,
trial_success_count bigint,
score_max float,
new_senior_class_info bigint,
filled_new_rate bigint,
minmax_score float,
filled_province_byphone bigint,
message string,
enable_trial_count bigint,
new_new_teached_age float,
filled_new_new_rate bigint,
new_new_student_sex bigint,
new_self_evaluation bigint,
filled_new_student_source bigint,
teacher_sex bigint,
filled_new_teacher_city_class bigint,
new_student_present_province bigint,
new_teacher_city_class bigint
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_ml_trial_lesson_recommend_model_b set tblproperties('es.resource' = 'aliyun_ml_trial_lesson_recommend_model_b_20181213/aliyun_ml_trial_lesson_recommend_model_b_type');

insert overwrite table ods_es.es_ml_trial_lesson_recommend_model_b partition(dt='20181213')
select * from tmp.es_ml_trial_lesson_recommend_model_b;

--9.trial_lesson_recommend_assign_model
CREATE EXTERNAL TABLE tmp.es_ml_trial_lesson_recommend_assign_model(
date_time string,
order_id bigint,
student_id bigint,
teacher_id bigint,
new_student_source bigint,
new_student_grade bigint,
new_student_city_class bigint,
lm_informal_teached_lesson_count bigint,
rf_prob float,
lgb_prob float,
xgb_prob float,
gbdt_prob float,
catboost_prob float,
effective_communication_count bigint,
teached_trial_course_count bigint,
senior_school_info bigint,
province_byphone bigint,
score_min float,
new_new_rate float,
score_mean float,
teacher_sex float,
new_teacher_daily_audition_count float,
new_new_self_evaluation float,
new_new_teached_age float,
new_teacher_daily_audition_success_count bigint,
taught_total_time bigint,
first_trial_course bigint,
teached_trial_success_student_count bigint,
teacher_audition_success_rate bigint,
grade_subject bigint,
new_teacher_plan_course_interval bigint,
new_learning_target bigint,
first_tkod_tifl_count bigint,
new_class_rank bigint,
new_new_student_sex bigint,
new_coil_in bigint
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_ml_trial_lesson_recommend_assigin_model_20181219/aliyun_ml_trial_lesson_recommend_assigin_model_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true'
);

CREATE EXTERNAL TABLE ods_es.es_ml_trial_lesson_recommend_assign_model(
date_time string,
order_id bigint,
student_id bigint,
teacher_id bigint,
new_student_source bigint,
new_student_grade bigint,
new_student_city_class bigint,
lm_informal_teached_lesson_count bigint,
rf_prob float,
lgb_prob float,
xgb_prob float,
gbdt_prob float,
catboost_prob float,
effective_communication_count bigint,
teached_trial_course_count bigint,
senior_school_info bigint,
province_byphone bigint,
score_min float,
new_new_rate float,
score_mean float,
teacher_sex float,
new_teacher_daily_audition_count float,
new_new_self_evaluation float,
new_new_teached_age float,
new_teacher_daily_audition_success_count bigint,
taught_total_time bigint,
first_trial_course bigint,
teached_trial_success_student_count bigint,
teacher_audition_success_rate bigint,
grade_subject bigint,
new_teacher_plan_course_interval bigint,
new_learning_target bigint,
first_tkod_tifl_count bigint,
new_class_rank bigint,
new_new_student_sex bigint,
new_coil_in bigint
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_ml_trial_lesson_recommend_assign_model set tblproperties('es.resource' = 'aliyun_ml_trial_lesson_recommend_assigin_model_20181213/aliyun_ml_trial_lesson_recommend_assigin_model_type');

insert overwrite table ods_es.es_ml_trial_lesson_recommend_assign_model partition(dt='20181213')
select * from tmp.es_ml_trial_lesson_recommend_assign_model;


--10.trial_lesson_batch_response
CREATE EXTERNAL TABLE tmp.es_ml_trial_lesson_batch_response(
date_time string,
order_id bigint,
teacher_id bigint,
batch bigint,
code bigint,
message string,
time_interval bigint,
notice_type bigint
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_ml_trial_lesson_batch_response_20181219/aliyun_ml_trial_lesson_batch_response_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true'
);

CREATE EXTERNAL TABLE ods_es.es_ml_trial_lesson_batch_response(
date_time string,
order_id bigint,
teacher_id bigint,
batch bigint,
code bigint,
message string,
time_interval bigint,
notice_type bigint
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_ml_trial_lesson_batch_response set tblproperties('es.resource' = 'aliyun_ml_trial_lesson_batch_response_20181213/aliyun_ml_trial_lesson_batch_response_type');

insert overwrite table ods_es.es_ml_trial_lesson_batch_response partition(dt='20181213')
select * from tmp.es_ml_trial_lesson_batch_response;

--11.trail_lesson_lesson_response
CREATE EXTERNAL TABLE tmp.es_ml_trial_lesson_lesson_response(
date_time string,
order_id bigint,
teacher_id bigint,
code bigint,
message string,
notice_type bigint,
lesson_begin_time string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_ml_trial_lesson_lesson_response_20181219/aliyun_ml_trial_lesson_lesson_response_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true'
);

CREATE EXTERNAL TABLE ods_es.es_ml_trial_lesson_lesson_response(
date_time string,
order_id bigint,
teacher_id bigint,
code bigint,
message string,
notice_type bigint,
lesson_begin_time string
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_ml_trial_lesson_lesson_response set tblproperties('es.resource' = 'aliyun_ml_trial_lesson_lesson_response_20181213/aliyun_ml_trial_lesson_lesson_response_type');

insert overwrite table ods_es.es_ml_trial_lesson_lesson_response partition(dt='20181213')
select * from tmp.es_ml_trial_lesson_lesson_response;

--12.xue nginx
CREATE EXTERNAL TABLE tmp.es_xue_nginx(
date_time string,
remote_addr string,
arg_room_id string,
upstream_addr string,
body_bytes_sent bigint,
uid_set string,
http_uuid string,
request_method string,
http_host string,
uri string,
uid_got string,
http_user_agent string,
args string,
time_iso8601 string,
request_time float,
http_referer string,
http_x_hf_learn_session_id string,
upstream_name string,
http_x_forwarded_for string,
upstream_response_time string,
http_cookie string,
server_protocol string,
status bigint
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_xue_nginx_20181219/aliyun_xue_nginx_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true',
'es.mapping.date.rich' = 'false'
);

CREATE EXTERNAL TABLE ods_es.es_xue_nginx(
date_time string,
remote_addr string,
arg_room_id string,
upstream_addr string,
body_bytes_sent bigint,
uid_set string,
http_uuid string,
request_method string,
http_host string,
uri string,
uid_got string,
http_user_agent string,
args string,
time_iso8601 string,
request_time float,
http_referer string,
http_x_hf_learn_session_id string,
upstream_name string,
http_x_forwarded_for string,
upstream_response_time string,
http_cookie string,
server_protocol string,
status bigint
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_xue_nginx set tblproperties('es.resource' = 'aliyun_xue_nginx_20181213/aliyun_xue_nginx_type');

insert overwrite table ods_es.es_xue_nginx partition(dt='20181213')
select * from tmp.es_xue_nginx;

--13.study video bypass
CREATE EXTERNAL TABLE tmp.es_study_video_bypass(
date_time string,
abtest_version bigint,
lesson_plan_id bigint,
student_id bigint
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_study_video_bypass_20181219/aliyun_study_video_bypass_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true',
'es.mapping.date.rich' = 'false'
);


CREATE EXTERNAL TABLE ods_es.es_study_video_bypass(
date_time string,
abtest_version bigint,
lesson_plan_id bigint,
student_id bigint
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_study_video_bypass set tblproperties('es.resource' = 'aliyun_study_video_bypass_20181213/aliyun_study_video_bypass_type');

insert overwrite table ods_es.es_study_video_bypass partition(dt='20181213')
select * from tmp.es_study_video_bypass;

--14.study robot pad
CREATE EXTERNAL TABLE tmp.es_study_robot_pad(
date_time string,
lesson_plan_id bigint,
user_id bigint,
user_type bigint,
device_sdk_version string,
device_version string,
connect_status string,
source string,
device_mode_no string,
app_name string,
source_id string,
order_version string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_study_robotpad_20181219/aliyun_study_robotpad_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true',
'es.mapping.date.rich' = 'false'
);


CREATE EXTERNAL TABLE ods_es.es_study_robot_pad(
date_time string,
lesson_plan_id bigint,
user_id bigint,
user_type bigint,
device_sdk_version string,
device_version string,
connect_status string,
source string,
device_mode_no string,
app_name string,
source_id string,
order_version string
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_study_robot_pad set tblproperties('es.resource' = 'aliyun_study_robotpad_20181213/aliyun_study_robotpad_type');

insert overwrite table ods_es.es_study_robot_pad partition(dt='20181213')
select * from tmp.es_study_robot_pad;


--15.study lessonPlanId quizId map
CREATE EXTERNAL TABLE tmp.es_study_lessonplanid_quizid_map(
date_time string,
lesson_plan_id string,
quiz_id string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_study_lesson_plan_id_quiz_20181219/aliyun_study_lesson_plan_id_quiz_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true',
'es.mapping.date.rich' = 'false'
);

CREATE EXTERNAL TABLE ods_es.es_study_lessonplanid_quizid_map(
date_time string,
lesson_plan_id string,
quiz_id string
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_study_lessonplanid_quizid_map set tblproperties('es.resource' = 'aliyun_study_lesson_plan_id_quiz_20181213/aliyun_study_lesson_plan_id_quiz_type');

insert overwrite table ods_es.es_study_lessonplanid_quizid_map partition(dt='20181213')
select * from tmp.es_study_lessonplanid_quizid_map;


--16.study access
CREATE EXTERNAL TABLE tmp.es_study_access(
date_time string,
headers string,
referer string,
method string,
ip string,
params string,
ua string,
uri string,
uuid string,
response_size string,
action string,
http_status string,
session_device string,
take_time string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_study_access_20181219/aliyun_study_access_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true',
'es.mapping.date.rich' = 'false'
);

CREATE EXTERNAL TABLE ods_es.es_study_access(
date_time string,
headers string,
referer string,
method string,
ip string,
params string,
ua string,
uri string,
uuid string,
response_size string,
action string,
http_status string,
session_device string,
take_time string
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_study_access set tblproperties('es.resource' = 'aliyun_study_access_20181213/aliyun_study_access_type');

insert overwrite table ods_es.es_study_access partition(dt='20181213')
select * from tmp.es_study_access;

--17.study origin
CREATE EXTERNAL TABLE tmp.es_study_origin(
date_time string,
logmessage string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_study_origin_20181219/aliyun_study_origin_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true',
'es.mapping.date.rich' = 'false'
);

CREATE EXTERNAL TABLE ods_es.es_study_origin(
date_time string,
logmessage string
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

alter table tmp.es_study_origin set tblproperties('es.resource' = 'aliyun_study_origin_20181213/aliyun_study_origin_type');

insert overwrite table ods_es.es_study_origin partition(dt='20181213')
select * from tmp.es_study_origin;





