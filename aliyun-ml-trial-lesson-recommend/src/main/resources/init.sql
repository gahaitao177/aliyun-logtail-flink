
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