cd Queries/Query2/RDD
echo "Executing query 2 RDD"
spark-submit --deploy-mode client query2_rdd.py 100 --num-executors 4 > output.txt