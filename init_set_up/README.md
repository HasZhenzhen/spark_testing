To run a standalone program in spark,

1 create the sbt configuration file following the example.

2 organize the files as the following tree structure:

$ find .

.

./example.sbt

./src

./src/main

./src/main/scala

./src/main/scala/SparkPi.scala

3 package a jar containing your application:

    sbt package
    
   To recompile: 
   
    sbt clean compile 

4 use spark-submit to run your application

  spark-submit \
  
--class main-class \

--master master-url \

--deploy-mode deploy-mode \

... #other options

application-jar \

[application-arguments]

example usage:

/root/spark/bin/spark-submit --class "SparkPi" --master spark://ec2-masterip.compute-1.amazonaws.com:7077 /root/example/target/scala-2.10/sparkpi_2.10-1.0.jar
