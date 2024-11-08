from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest
from pathlib import Path
import time
import io
import json
import datetime
import pandas as pd
from google.cloud import bigquery_datatransfer_v1
from google.protobuf.timestamp_pb2 import Timestamp


class Big_query:
    def __init__(self, path=None):
        self.credentials = service_account.Credentials.from_service_account_file(
            path or Path(Path(__file__).parent, "credentials", "bq-key.json"), scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.project_id = self.credentials.project_id

    def get_bg_client(self):
        return bigquery.Client(credentials=self.credentials, project=self.project_id,)

    def stream_to_bq(self, table_id, object_report_list: list) -> str:
        bq = self.get_bg_client()
        # split object_report_list to chunks of size max 5000 - limit 10k per request, recommend 500
        chunks = [object_report_list[offs:offs + 5000] for offs in range(0, len(object_report_list), 5000)]
        for i, chunk in enumerate(chunks):
            # wait 1 sec every 20 chunk - limit 100k rows per second
            if (i + 1) % 20 == 0:
                time.sleep(1)
            errors = bq.insert_rows_json(table_id, chunk)  # Make an API request.
            if errors != []:
                raise ConnectionError(f'Encountered errors while inserting rows to table {self.project_id}.{table_id} ({i+1}/{len(chunks)}):\n{errors}')

        return f"New rows have been added to table {self.project_id}.{table_id}"

    def load_to_bq_as_file(self, table_id, object_report_list: list, mode, schema=None) -> str:
        # convert list of dicts to ND JSON
        list_of_jsons = [json.dumps(record, default=str) for record in object_report_list]
        stringio_data = io.StringIO('\n'.join(list_of_jsons))

        return f'{self._load_to_bq(file=stringio_data, table_id=table_id, mode=mode, schema=schema)}, {len(list_of_jsons)} rows loaded.'

    def load_from_csv(self, table_id: str, path_to_csv: Path, mode: str, schema: dict = None, skip_header=False) -> str:
        with open(path_to_csv, 'rb') as f:
            file_len = sum(1 for _ in f)
        with open(path_to_csv, 'rb') as f:
            return f"{self._load_to_bq(table_id=table_id, file=f, mode=mode, file_type='csv', schema=schema, skip_header=skip_header)}, {file_len} rows loaded."

    def load_from_parquet(self, table_id: str, path_to_csv: Path, mode: str, schema: dict = None) -> str:
        with open(path_to_csv, 'rb') as f:
            return f"{self._load_to_bq(table_id=table_id, file=f, mode=mode, file_type='parquet', schema=schema)}, many rows loaded."

    def _load_to_bq(self, table_id, file, mode, file_type='nd json', schema=None, skip_header=False) -> str:
        # job config
        # https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload
        source_formats = {
            'nd json': bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            'csv': bigquery.SourceFormat.CSV,
            'parquet': bigquery.SourceFormat.PARQUET
        }
        job_config = bigquery.LoadJobConfig(
            source_format=source_formats[file_type]
        )
        if schema:
            job_config.schema = self.format_schema(schema)
        else:
            job_config.autodetect = True
        if mode == 'append':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        elif mode == 'truncate':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif mode == 'empty':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        else:
            raise ValueError(f'{mode} is not valid mode. Choose append, truncate or empty')

        if skip_header and file_type == 'csv':
            job_config.skip_leading_rows = 1

        bq = self.get_bg_client()
        job = bq.load_table_from_file(file, table_id, job_config=job_config)
        try:
            job.result()
        except BadRequest as e:
            error_message = '\n'.join([str(err) for err in job.errors])
            raise BadRequest(f'{e}\nerrors[]:\n{error_message}')
        return f'Load to table {self.project_id}.{table_id} in {mode} mode succeed'

    def load_from_dataframe(self, table_id: str, dataframe: pd.DataFrame, mode: str, schema: list = None) -> str:
        job_config = bigquery.LoadJobConfig()
        if schema:
            job_config.schema = self.format_schema(schema)
        else:
            job_config.autodetect = True
        if mode == 'append':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        elif mode == 'truncate':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif mode == 'empty':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        else:
            raise ValueError(f'{mode} is not valid mode. Choose append, truncate or empty')
        bq = self.get_bg_client()
        job = bq.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
        try:
            job.result()
        except BadRequest as e:
            error_message = '\n'.join([str(err) for err in job.errors])
            raise BadRequest(f'{e}\nerrors[]:\n{error_message}')
        return f'Load to table {self.project_id}.{table_id} in {mode} mode succeed, {len(dataframe)} rows loaded.'

    @staticmethod
    def format_schema(schema: list) -> list:
        formatted_schema = []
        for row in schema:
            try:
                if row['type'] == "RECORD":
                    formatted_schema.append(
                        bigquery.SchemaField(row['name'], row['type'], mode=row['mode'],
                                             fields=Big_query.format_schema(row['fields'])
                                             ))
                else:
                    formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], mode=row['mode']))
            except KeyError as e:
                raise ValueError(f'Missing field in table schema - {e} in column: {row}\n"name", "type" and "mode" are required')
        return formatted_schema

    def clear_bq_table(self, table_id: str) -> str:
        query = f'''CREATE OR REPLACE TABLE `{self.project_id}.{table_id}` AS
    SELECT * FROM `{self.project_id}.{table_id}` LIMIT 0'''
        bq = self.get_bg_client()
        query_job = bq.query(query)
        query_job.result()

        return f"Table {self.project_id}.{table_id} is cleared"

    def get_last_push_date(self, table_id: str, date_column: str) -> datetime.datetime:
        if self.is_empty(table_id):
            return None
        query = f'''SELECT max({date_column}) as date FROM `{self.project_id}.{table_id}`'''
        query_job = self.query(query)
        for res in query_job:
            return res.get('date')

    def is_empty(self, table_id: str) -> bool:
        query = f'''SELECT count(*) as count FROM `{self.project_id}.{table_id}`'''
        query_job = self.query(query)
        for res in query_job:
            return res.get('count') == 0

    def query(self, query: str) -> bigquery.table.RowIterator:
        bq = self.get_bg_client()
        query_job = bq.query(query)
        return query_job.result()

    def dml_query(self, query: str) -> str:
        if ';' in query and not query.split(';')[1].strip() == '':
            raise ValueError('Multi-statement queries not supported. Use Big_query.query() instead')
        bq = self.get_bg_client()
        query_job = bq.query(query)
        query_job.result()
        table = f'{query_job.destination.project}.{query_job.destination.dataset_id}.{query_job.destination.table_id}'
        operatnions = [
            {'v': query_job.dml_stats.inserted_row_count, 's': f'{query_job.dml_stats.inserted_row_count} rows inserted'},
            {'v': query_job.dml_stats.deleted_row_count, 's': f'{query_job.dml_stats.deleted_row_count} rows deleted'},
            {'v': query_job.dml_stats.updated_row_count, 's': f'{query_job.dml_stats.updated_row_count} rows updated'}
        ]
        return f'{query_job.num_dml_affected_rows} rows affected in table {table}, {", ".join([x["s"] for x in operatnions if x["v"] > 0])}'

    def update_view(self, view: str, query: str) -> str:
        client = self.get_bg_client()
        view = client.get_table(view)
        view.view_query = query
        view = client.update_table(view, ["view_query"])
        return f"Updated {view.table_type}: {view.reference}"

    def run_scheduled_queries_manualy(self, location, transfer_config_id):
        project_id = "170847861615"
        client = bigquery_datatransfer_v1.DataTransferServiceClient(credentials=self.credentials)
        parent = f'projects/{project_id}/locations/{location}/transferConfigs/{transfer_config_id}'
        start_time = Timestamp(seconds=int(time.time() + 10))
        client.start_manual_transfer_runs({'parent': parent, 'requested_run_time': start_time})
        return f'transfer {transfer_config_id} succesfull'


if __name__ == '__main__':
    print(Big_query(Path('credentials', 'bq-key.json')).is_empty('skiddou.transaction_sources'))
"""
from big_query import Big_query
bq = Big_query()
t = bq.dml_query('insert into `zmaterializowane_randomy.test` (dupa) values (10)')
"""
