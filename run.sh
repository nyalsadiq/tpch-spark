printf "\nbuilding evaluation program with sbt\n\n"

#sbt package

declare -a arr=("FIFO" "LIFO" "LFU" "LRU" "TINY")

export SQL_CACHE_SIZE=5

for strategy in "${arr[@]}"
do
   export SQL_CACHING_SCHEME=$strategy
   printf "using $SQL_CACHING_SCHEME caching scheme\n"
   printf "using CacheManager size of $SQL_CACHE_SIZE\n\n"

   printf -- "---------------RUNNING EVALUATION FOR $SQL_CACHING_SCHEME-------------\n\n"
   #spark-submit --class "main.scala.TpchQuery" --master local target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar 1-10
   printf -- "---------------FINISHED EVALUATION FOR $SQL_CACHING_SCHEME-------------\n\n"
done

# ./SparkEagerCaching/bin/spark-shell --conf spark.block.scheme=LRU --conf spark.sql.scheme=LFU --conf spark.sq.size=10
