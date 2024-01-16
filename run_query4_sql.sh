cd Queries/Query4/SQL
echo "Executing query 4 SQL"
spark-submit --deploy-mode client query4_sql.py 100 --num-executors 4 > output.txt
