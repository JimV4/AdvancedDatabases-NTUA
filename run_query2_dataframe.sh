cd Queries/Query2/DataFrame 
echo "Executing query 2 DataFrame" 
spark-submit --deploy-mode client query2_dataframe.py 100 --num-executors 4 > output.txt
