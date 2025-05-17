from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import psycopg2
import snowflake.connector

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def transfer_table_to_snowflake():
    # PostgreSQL connection
    pg_conn = psycopg2.connect(
        host='postgres',
        port=5432,
        database='postgres',
        user='postgres',
        password='postgres'
    )
    pg_cursor = pg_conn.cursor()

    # Snowflake connection
    sf_conn = snowflake.connector.connect(
        user='MUJIDON',
        password='6122002Muji123456',
        account='fa17772.me-central2.gcp',
        warehouse='COMPUTE_WH',
        database='LOAN_DATASET',
        schema='LOAN_ANALYTICS'
    )
    sf_cursor = sf_conn.cursor()

    pg_tables = {
        'STAGING_LOANS': '''
            loanId INT, memberId INT, date DATE, purpose STRING, isJointApplication BOOLEAN,
            loanAmount FLOAT, term FLOAT, interestRate FLOAT, monthlyPayment FLOAT,
            grade STRING, loanStatus STRING
        ''',
        'STAGING_LOANS_PROD': '''
            loanId INT, memberId INT, date DATE, purpose STRING, isJointApplication BOOLEAN,
            loanAmount FLOAT, term FLOAT, interestRate FLOAT, monthlyPayment FLOAT,
            grade STRING, loanStatus STRING
        ''',
        'STAGING_BORROWERS': '''
            memberId INT, residentialState STRING, yearsEmployment STRING, homeOwnership STRING,
            annualIncome FLOAT, incomeVerified STRING, dtiRatio FLOAT, lengthCreditHistory FLOAT,
            numTotalCreditLines FLOAT, numOpenCreditLines FLOAT, numOpenCreditLines1Year FLOAT,
            revolvingBalance FLOAT, revolvingUtilizationRate FLOAT, numDerogatoryRec FLOAT,
            numDelinquency2Years FLOAT, numChargeoff1year FLOAT, numInquiries6Mon FLOAT,
            yearsEmployment_numeric FLOAT
        ''',
        'STAGING_BORROWERS_PROD': '''
            memberId INT, residentialState STRING, yearsEmployment STRING, homeOwnership STRING,
            annualIncome FLOAT, incomeVerified STRING, dtiRatio FLOAT, lengthCreditHistory FLOAT,
            numTotalCreditLines FLOAT, numOpenCreditLines FLOAT, numOpenCreditLines1Year FLOAT,
            revolvingBalance FLOAT, revolvingUtilizationRate FLOAT, numDerogatoryRec FLOAT,
            numDelinquency2Years FLOAT, numChargeoff1year FLOAT, numInquiries6Mon FLOAT,
            yearsEmployment_numeric FLOAT
        '''
    }

    for table_name, sf_schema in pg_tables.items():
        print(f"🔁 Transferring `{table_name}`...")
        df = pd.read_sql_query(f"SELECT * FROM {table_name.lower()}", pg_conn)

        # Create if not exists
        sf_cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({sf_schema})")

        # Clear existing data
        sf_cursor.execute(f"TRUNCATE TABLE {table_name}")

        # Insert data
        if not df.empty:
            insert_stmt = f"INSERT INTO {table_name} VALUES ({','.join(['%s'] * len(df.columns))})"
            data = [tuple(None if pd.isna(x) else x for x in row) for _, row in df.iterrows()]
            for i in range(0, len(data), 1000):
                sf_cursor.executemany(insert_stmt, data[i:i + 1000])
                print(f"✅ Inserted {len(data[i:i + 1000])} rows into `{table_name}`")

    pg_cursor.close()
    sf_cursor.close()
    pg_conn.close()
    sf_conn.close()
    print("🎉 All tables transferred successfully!")

with DAG(
    dag_id='pg_to_snowflake_dag',
    default_args=default_args,
    description='Transfer data from Postgres to Snowflake safely',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
) as dag:

    migrate = PythonOperator(
        task_id='transfer_pg_to_snowflake',
        python_callable=transfer_table_to_snowflake
    )

    migrate
