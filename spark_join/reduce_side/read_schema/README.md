New feature since Spark 1.1.0 Testing.

Instead of hardcoded in programs, the s3n dirs and schemas will all be read from json files.

dirFile: json file containing s3n dir to tables for joining.

schemaFile: json file holding the schemas for each table.

After compilation, run with /root/spark/bin/spark-submit --class "SparkJoin2" --master spark://ec2-54-211-179-185.compute-1.amazonaws.com:7077 /root/zhenzhentest/schematest/target/scala-2.10/spark_join_sql_2.10-1.1.jar /root/zhenzhentest/schematest/dirFile /root/zhenzhentest/schematest/schemaFile

