cd Queries/Query1/DataFrame
echo "Executing query 1 Dataframe"
spark-submit --deploy-mode client query1_dataframe.py 100 --num-executors 4 > output.txt