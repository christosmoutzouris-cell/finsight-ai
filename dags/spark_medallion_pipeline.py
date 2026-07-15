from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "finsight",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

SPARK_SUBMIT = (
    "docker exec finsight-spark-master "
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--packages org.postgresql:postgresql:42.7.3 "
)

with DAG(
    dag_id="spark_medallion_pipeline",
    description="Bronze → Silver → Gold → dbt → Sentiment",
    default_args=default_args,
    schedule= None, #"0 22 * * 1-5",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=["finsight", "spark", "medallion", "dbt", "ai"],
) as dag:

    fix_spark_permissions = BashOperator(
    task_id="fix_spark_permissions",
    bash_command=(
        "docker exec -u root finsight-spark-master mkdir -p /home/spark/.ivy2/cache && "
        "docker exec -u root finsight-spark-master chmod -R 777 /home/spark/.ivy2"
    ),
)

    cleanup_old_data = BashOperator(
    task_id="cleanup_old_data",
    bash_command="""
        docker exec finsight-postgres psql -U finsight -d finsight_db -c "
        DELETE FROM stock_prices WHERE ingested_at < NOW() - INTERVAL '3 days';
        DELETE FROM stock_prices_silver WHERE event_time < NOW() - INTERVAL '3 days';
        DELETE FROM sentiment_scores WHERE analyzed_at < NOW() - INTERVAL '30 days';
        DELETE FROM price_predictions WHERE predicted_at < NOW() - INTERVAL '30 days';
        "
    """,
)
    update_yfinance = BashOperator(
    task_id="update_yfinance",
    bash_command=(
        "docker exec finsight-producer pip install --upgrade yfinance -q && "
        "docker restart finsight-producer && "
        "sleep 10"
    ),
)
    
    
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=SPARK_SUBMIT + "/opt/spark_jobs/bronze_to_silver.py",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=SPARK_SUBMIT + "/opt/spark_jobs/silver_to_gold.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "docker exec finsight-dbt pip install 'protobuf==4.25.3' -q && "
            "docker exec finsight-dbt dbt run && "
            "docker exec finsight-dbt dbt test"
        ),
    )

    sentiment_analysis = BashOperator(
    task_id="sentiment_analysis",
    bash_command=(
        "docker run --rm "
        "--network finsight-ai_default "
        "-v sentiment_model:/app/models "
        "finsight-ai-sentiment "
        "python train_and_predict.py"
    ),
)
    
    lstm_prediction = BashOperator(
    task_id="lstm_prediction",
    bash_command=(
        "docker run --rm "
        "--network finsight-ai_default "
        "finsight-ai-lstm "
        "python lstm_predict.py"
    ),
)
    
    lstm_evaluate = BashOperator(
    task_id="lstm_evaluate_predictions",
    bash_command=(
        "docker run --rm "
        "--network finsight-ai_default "
        "finsight-ai-lstm "
        "python evaluate_predictions.py"
    ),
)
    
    nn_sentiment = BashOperator(
    task_id="nn_sentiment",
    bash_command=(
        "docker run --rm "
        "--network finsight-ai_default "
        "-v finsight-ai_nn_models:/app/models "
        "finsight-ai-neural-networks "
        "python train_sentiment.py"
    ),
)

    nn_price_direction = BashOperator(
    task_id="nn_price_direction",
    bash_command=(
        "docker run --rm "
        "--network finsight-ai_default "
        "-v finsight-ai_nn_models:/app/models "
        "finsight-ai-neural-networks "
        "python train_price_direction.py"
    ),
)
    
    data_quality_report = BashOperator(
    task_id="data_quality_report",
    bash_command="""
        echo "==================== DATA QUALITY REPORT ====================" &&
        echo "" &&
        echo "--- BRONZE: stock_prices ---" &&
        docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT count(*) as total_rows FROM stock_prices;" &&
        echo "" &&
        echo "--- DAILY SUMMARY ---" &&
        docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT symbol, date, close, daily_return, created_at FROM daily_stock_summary ORDER BY created_at DESC LIMIT 5;" &&
        echo "" &&
        echo "--- SILVER: stock_prices_silver ---" &&
        docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT count(*) as total_rows FROM stock_prices_silver;" &&
        echo "" &&
        echo "--- GOLD: symbol_snapshot ---" &&
        docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT symbol, price, ma5, ma20, ma5_signal, pct_change FROM gold_symbol_snapshot;" &&
        echo "" &&
        echo "--- GOLD: daily_metrics ---" &&
        docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT symbol, date, close, price_volatility, tick_count FROM gold_daily_metrics ORDER BY date DESC LIMIT 5;" &&
        echo "" &&
        echo "--- DBT: fct_stock_performance ---" &&
        docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT symbol, date, close, cumulative_return, daily_rank FROM fct_stock_performance ORDER BY date DESC, daily_rank LIMIT 5;" &&
        echo "" &&
        echo "--- DBT: dim_symbol_stats ---" &&
        docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT symbol, total_days, avg_daily_return, volatility, positive_days, negative_days FROM dim_symbol_stats;" &&
        echo "" &&
        echo "--- SENTIMENT SCORES ---" &&
        docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT symbol, sentiment, confidence, analyzed_at FROM sentiment_scores ORDER BY analyzed_at DESC LIMIT 10;" &&
        echo "" &&
        echo "--- LSTM: price_predictions ---" &&
        docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT symbol, current_price, predicted_price, predicted_change, predicted_direction, predicted_at FROM price_predictions ORDER BY predicted_at DESC LIMIT 5;" &&
        echo "--- LSTM: accuracy report ---" &&
        docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT symbol, COUNT(*) as total, SUM(CASE WHEN direction_correct THEN 1 END) as correct, ROUND(AVG(price_error)::numeric, 2) as avg_error FROM price_predictions WHERE evaluated_at IS NOT NULL GROUP BY symbol;" &&
        echo "--- NN SENTIMENT SCORES ---" &&
        docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT symbol, nn_sentiment, nn_confidence, analyzed_at FROM sentiment_nn_scores ORDER BY analyzed_at DESC LIMIT 10;" &&
        echo "" &&
        echo "--- NN PRICE DIRECTION ---" &&
        docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT symbol, current_price, predicted_dir, confidence, prob_up, prob_down, predicted_at FROM price_direction_predictions ORDER BY predicted_at DESC LIMIT 10;" &&
        echo "=============================================================="
    """,
)

fix_spark_permissions >> cleanup_old_data >> update_yfinance >> bronze_to_silver >> silver_to_gold >> dbt_run >> sentiment_analysis >> nn_sentiment >> nn_price_direction >> lstm_prediction >> lstm_evaluate >> data_quality_report 

    
    
    
'''
# Δημιουργεί το documentation
docker exec finsight-dbt dbt docs generate

# Ξεκινάει web server στο port 8083
docker exec -d -p 8083:8080 finsight-dbt dbt docs serve --port 8080

Μετά άνοιξε: http://localhost:8083
Θα δεις:

Αριστερά: λίστα με όλα τα models (stg_stock_prices, fct_stock_performance κλπ)
Κλίκ σε model → βλέπεις στήλες, tests, SQL code
Κάτω δεξιά: "View Lineage Graph" → γράφημα που δείχνει πώς συνδέονται τα models


Επίσης για γρήγορο έλεγχο χωρίς UI:
powershell# Δες τα test results
docker exec finsight-dbt dbt test

# Δες τα compiled SQL models
docker exec finsight-dbt dbt compile
'''
