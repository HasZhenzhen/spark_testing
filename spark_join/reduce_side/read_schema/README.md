New feature since Spark 1.1.0 Testing.

Instead of hardcoded in programs, the s3n dirs and schemas will all be read from json files.

dirFile: json file containing s3n dir to tables for joining.

schemaFile: json file holding the schemas for each table.

After compilation, run with /root/spark/bin/spark-submit --class " " --master spark://ec2-masterip.compute-1.amazonaws.com:7077 /dir/to/jar/somejar.jar /dir/to/file/dirFile /dir/to/schema/schemaFile


