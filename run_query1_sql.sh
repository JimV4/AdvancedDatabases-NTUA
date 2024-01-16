cd Queries/Query1/SQL
echo "Executing query 1 SQL"
spark-submit --deploy-mode client query1_sql.py 100 --num-executors 4 > output.txt