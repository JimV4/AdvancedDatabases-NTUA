cd CreateDataframe 
echo "Executing Create Dataframe"
spark-submit --deploy-mode client create_dataframe.py 100 --num-executors 4 > output.txt
