
select a.order_id from
(select distinct order_id from ods.trial_lesson_recommend_model_a where dt='20181220')a left join

(select distinct order_id from ods_es.es_ml_trial_lesson_recommend_model_a where dt = '20181220')b on a.order_id = b.order_id
where b.order_id is null limit 10;


select a.order_id from
(select distinct order_id from ods.trial_lesson_recommend_model_b where dt='20181220')a left join

(select distinct order_id from ods_es.es_ml_trial_lesson_recommend_model_b where dt = '20181220')b on a.order_id = b.order_id
where b.order_id is null limit 10;