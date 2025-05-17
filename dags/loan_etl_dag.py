from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
import pandas as pd
import psycopg2

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def load_csv_to_postgres(file_path, table, columns, types, chunk_size=10000):
    log = LoggingMixin().log
    conn = psycopg2.connect(
        host='postgres',
        database='postgres',
        user='postgres',
        password='postgres'
    )
    cursor = conn.cursor()

    # Create table if not exists
    create_stmt = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        {', '.join(f'{col} {typ}' for col, typ in zip(columns, types))}
    );
    """
    cursor.execute(create_stmt)
    conn.commit()

    insert_stmt = f"""
    INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})
    """

    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        rows = []
        for _, row in chunk.iterrows():
            try:
                values = []
                for col in columns:
                    val = row.get(col)
                    if pd.isna(val):
                        values.append(None)
                    elif col == 'isJointApplication':
                        values.append(bool(val))
                    else:
                        values.append(val)
                rows.append(tuple(values))
            except Exception as e:
                log.warning(f"Skipping row due to error: {e}")

        try:
            cursor.executemany(insert_stmt, rows)
            conn.commit()
            log.info(f"Inserted {len(rows)} rows into {table}")
        except Exception as e:
            log.error(f"Failed inserting chunk into {table}: {e}")
            conn.rollback()

    cursor.close()
    conn.close()

with DAG(
    dag_id='file_ingestion_etl_dag',
    default_args=default_args,
    description='Load CSVs from new_data folder into Postgres with chunking',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'csv', 'postgres']
) as dag:

    loan_cols = ['loanId', 'memberId', 'date', 'purpose', 'isJointApplication', 'loanAmount', 'term', 'interestRate', 'monthlyPayment', 'grade', 'loanStatus']
    loan_types = ['BIGINT', 'BIGINT', 'DATE', 'TEXT', 'BOOLEAN', 'FLOAT', 'FLOAT', 'FLOAT', 'FLOAT', 'TEXT', 'TEXT']

    borrower_cols = ['memberId', 'residentialState', 'yearsEmployment', 'homeOwnership', 'annualIncome', 'incomeVerified',
                     'dtiRatio', 'lengthCreditHistory', 'numTotalCreditLines', 'numOpenCreditLines',
                     'numOpenCreditLines1Year', 'revolvingBalance', 'revolvingUtilizationRate',
                     'numDerogatoryRec', 'numDelinquency2Years', 'numChargeoff1year', 'numInquiries6Mon',
                     'yearsEmployment_numeric']
    borrower_types = ['BIGINT', 'TEXT', 'TEXT', 'TEXT', 'FLOAT', 'TEXT',
                      'FLOAT', 'FLOAT', 'FLOAT', 'FLOAT', 'FLOAT', 'FLOAT', 'FLOAT',
                      'FLOAT', 'FLOAT', 'FLOAT', 'FLOAT', 'FLOAT']

    load_loan = PythonOperator(
        task_id='load_loan',
        python_callable=load_csv_to_postgres,
        op_args=['/opt/airflow/new_data/loan_df.csv', 'staging_loans', loan_cols, loan_types]
    )

    load_loan_prod = PythonOperator(
        task_id='load_loan_prod',
        python_callable=load_csv_to_postgres,
        op_args=['/opt/airflow/new_data/loan_prod_df.csv', 'staging_loans_prod', loan_cols, loan_types]
    )

    load_borrower = PythonOperator(
        task_id='load_borrower',
        python_callable=load_csv_to_postgres,
        op_args=['/opt/airflow/new_data/borrower_df.csv', 'staging_borrowers', borrower_cols, borrower_types]
    )

    load_borrower_prod = PythonOperator(
        task_id='load_borrower_prod',
        python_callable=load_csv_to_postgres,
        op_args=['/opt/airflow/new_data/borrower_prod_df.csv', 'staging_borrowers_prod', borrower_cols, borrower_types]
    )

    [load_loan, load_loan_prod, load_borrower, load_borrower_prod]
