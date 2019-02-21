
select a.order_id from
(select distinct order_id from ods.trial_lesson_recommend_model_a where dt='20181216')a left join

(select distinct order_id from ods_es.es_ml_trial_lesson_recommend_model_a where dt = '20181216')b on a.order_id = b.order_id
where b.order_id is null limit 10;


select a.order_id, counta, countb from
(select order_id, count(*) as counta from ods.trial_lesson_recommend_model_a where dt='20181217' group by order_id)a inner join
(select order_id, count(*) as countb from ods_es.es_ml_trial_lesson_recommend_model_a where dt='20181217' group by order_id)b on a.order_id = b.order_id
limit 10;


select a.order_id from
(select distinct order_id from ods.trial_lesson_recommend_model_b where dt='20181216')a left join

(select distinct order_id from ods_es.es_ml_trial_lesson_recommend_model_b where dt = '20181216')b on a.order_id = b.order_id
where b.order_id is null limit 10;


select a.lesson_plan_id, c1, c2 from
(select lesson_plan_id, count(*) as c1 from ods.ls_study_video_bypass where dt='20181227' group by lesson_plan_id)a inner join
(select lesson_plan_id, count(*) as c2 from ods_es.es_study_video_bypass where dt='20181227' group by lesson_plan_id)b on a.lesson_plan_id = b.lesson_plan_id
where c1 != c2 limit 10

