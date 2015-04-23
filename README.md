 Big Query ETL Tool
====================

Big Query ETL tool for creating Big Query table schema from avro schema and serializing avro objects to json.

Creating Big Query schema:
---------------------------
<pre>
hadoop jar bq-tool-1.0-jar-with-dependencies.jar com.rtbhouse.bq.avro.SchemaConverter hdfs:///user/myuser/avro_schema_file.avsc > bq_schema.bqsc
</pre>

Creating json which can be loaded to Big Query with "bq load" job.
------------------------------------------------------------------
<pre>
hadoop jar bq-tool-1.0-jar-with-dependencies.jar com.rtbhouse.bq.avro.AvroToJson com.rtbhouse.bq.avro.AvroToJson -s avro_schema_file.avsc -i /user/myuser/in_avro_files.avro -o /user/myuser/out_json_files

usage:  hadoop jar bq-tool-1.0-jar-with-dependencies.jar com.rtbhouse.bq.avro.AvroToJson com.rtbhouse.bq.avro.AvroToJson [<options>]
options:
-f,--file <arg>        Avro file or directory to be processed.
-m,--mapsize <arg>     Max split mapsize in MB.
-o,--output <arg>      HDFS output directory.
-r,--rowsize <arg>     Max json row size bytes.
-s,--avroschema <arg>  Avro schema file to be processed.
-u,--usage             Print usage.
</pre>
