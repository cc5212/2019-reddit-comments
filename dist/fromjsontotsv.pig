SET mapred.output.compress 'true';
SET mapred.output.compression.codec 'org.apache.hadoop.io.compress.BZip2Codec';
-- cargar datos
--raw_data = LOAD 'hdfs://cm:9000/uhadoop2019/bmat/Proyecto/RC_2012-04.bz2' USING JsonLoader('author:chararray, author_flair_css_class:chararray, author_flair_text:chararray, body:chararray, can_gild:boolean, controversiality:float, created_utc:long, distinguished:chararray , edited:boolean, gilded:int, id:chararray, is_submitter:boolean, link_id:chararray, parent_id:chararray, permalink:chararray, retrieved_on:long, score:int, stickied:boolean, subreddit:chararray, subreddit_id:chararray ');

--guardar datos de forma bonita
--STORE raw_data INTO "hdfs://cm:9000/uhadoop2019/bmat/Proyecto/bonito.bz2" USING PigStorage();

raw_data = LOAD 'hdfs://cm:9000/uhadoop2019/bmat/Proyecto/RC_2012-04.bz2' USING JsonLoader('author:chararray, author_flair_css_class:chararray, author_flair_text:chararray, body:chararray, can_gild:chararray, controversiality:chararray, created_utc:chararray, distinguished:chararray , edited:chararray, gilded:chararray, id:chararray, is_submitter:chararray, link_id:chararray, parent_id:chararray, permalink:chararray, retrieved_on:chararray, score:chararray, stickied:chararray, subreddit:chararray, subreddit_id:chararray ');


STORE raw_data INTO 'hdfs://cm:9000/uhadoop2019/bmat/Proyecto/datapig' USING PigStorage();
