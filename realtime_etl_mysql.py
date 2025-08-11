import mysql.connector
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from io import BytesIO
import os
import logging

# Set up logging
log_dir = os.path.join(os.getcwd(), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'etl_run.log')

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

try:
    logging.info("ETL job started.")

    # Connect to MySQL
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Chanikya15241*',
        database='retaildb'
    )
    query = "SELECT * FROM transactions;"
    df = pd.read_sql(query, conn)
    conn.close()

    # Transform
    df.dropna(inplace=True)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    df['amount_usd'] = df.apply(
        lambda row: round(row['amount'] * 1.1,
                          2) if row['currency'] == 'EUR' else row['amount'],
        axis=1
    )
    df['currency'] = 'USD'

    # S3 Upload
    s3 = boto3.client('s3')
    bucket = 'my-etl-raw-bucket1'

    for date, group in df.groupby(df['transaction_date'].dt.date):
        folder_path = f"raw/{date.strftime('%Y-%m-%d')}/transactions.parquet"
        table = pa.Table.from_pandas(group)
        buffer = BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        s3.upload_fileobj(buffer, bucket, folder_path)
        logging.info("Uploaded %d records to s3://%s/%s",
                     len(group), bucket, folder_path)

    logging.info("ETL job completed successfully.")

except Exception as e:
    logging.error("ETL job failed with error: %s", e)
