import requests
import pandas as pd
import datetime as dt
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound


API_URL = "https://api.covid19india.org/states_daily.json"
GOOGLE_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def connect_dataset(dataset_id):
    path = "./config/covidBigQueryProject-53452f2a6e67.json"
    credentials = service_account.Credentials.from_service_account_file(
        path, scopes=[GOOGLE_SCOPE])
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id)

    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))
    finally:
        return dataset_ref, client


def fetch_data(**args):
    pipeline_date = dt.datetime.strptime(args['pipeline_date'], '%Y-%m-%d')
    yes_date = pipeline_date.strftime('%d-%b-%y')
    res = requests.get(API_URL)
    res = res.json()
    pandas_df = pd.DataFrame(res['states_daily'])
    pandas_df = pandas_df[pandas_df['date'] == yes_date]
    pandas_df = pandas_df.melt(id_vars=["date", "status"], var_name="state", value_name="count")

    pandas_df['date'] = pd.to_datetime(pandas_df['date'], format="%d-%b-%y").dt.date
    pandas_df = pandas_df.rename(columns={'date': 'DateStr', 'state': 'State', 'count': 'Count', 'status': 'Status'})
    pandas_df = pandas_df[['DateStr', 'State', 'Count', 'Status']]
    pandas_df.to_csv('./CSV/'+pipeline_date.strftime('%Y-%m-%d')+'.csv', index=False)
    print('CSV generated.')
    args['ti'].xcom_push(key='csv_row_count', value=len(pandas_df))



def upload_csv_to_big_table(**args):
    print(args['partition_date'])
    dataset_ref, client = use_dataset(args['dataset_id'])
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.schema_update_options = ['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION']
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.skip_leading_rows = 1
    table_ref = dataset_ref.table(args['table_id']+"$"+args['partition_date'])
    filename = './CSV/{}.csv'.format(args['file_date'])
    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    try:
        job.result()
        print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_ref, table_ref))
        args['ti'].xcom_push(key='big_table_row_count', value=job.output_rows)
    except Exception as e:
        print(e)



def find_percentage(**args):
    total_rows = args['ti'].xcom_pull(dag_id='bq_covid_stats', key='csv_row_count')
    uploaded_rows = args['ti'].xcom_pull(dag_id='bq_covid_stats', key='big_table_row_count')
    print("Rows count: ", uploaded_rows)
    print("CSV rows: ", total_rows)
    try:
        total_percentage = (float(uploaded_rows)*100)/float(total_rows)
        print("Uploaded %: ", str(total_percentage)+'%')
    except Exception as e:
        print(e)
