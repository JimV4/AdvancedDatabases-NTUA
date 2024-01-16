cd Queries/Query3/Dataframe
echo "Executing query 3 Dataframe"
spark-submit --deploy-mode client query3_dataframe.py 100 --num-executors 4 > output.txt