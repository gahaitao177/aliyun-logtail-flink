add jar /home/bigdata/kehailin/elasticsearch-hadoop-6.4.2.jar;


-- es.resource 支持index带通配符*
CREATE EXTERNAL TABLE tmp.es_xue_study(
date_time string,
logmessage string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = 'node1-es.hfjy.red:17888,node2-es.hfjy.red:17888,node3-es.hfjy.red:17888',
'es.resource' = 'aliyun_xue_study_20181127/aliyun_xue_study_type',
'es.index.auto.create' = 'false',
'es.field.read.empty.as.null' = 'true',
'es.read.metadata' = 'true'
);

alter table tmp.es_xue_study set tblproperties('es.resource' = 'aliyun_xue_study_20181127/aliyun_xue_study_type');

create table ods.hive_xue_study(
date_time string,
logmessage string
)partitioned by(dt string)
stored as orc tblproperties ("orc.compress"="ZLIB");

insert overwrite table ods.hive_xue_study partition(dt='20181127')
select * from tmp.es_xue_study;