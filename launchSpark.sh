/opt/apache-spark/bin/spark-submit \
--class "iosr.lambda.arch.AviationStatistics" \
--master local[8] \
--driver-memory 15g \
target/scala-2.11/iosr-lambda-architecture-poc_2.11-1.0.jar
