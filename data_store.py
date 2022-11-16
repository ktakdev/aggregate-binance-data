from datetime import datetime, timedelta

from google.cloud import bigquery

from data import Kline


class KlineStore:
    def __init__(self, client: bigquery.Client, table: str):
        self.client = client
        self.table = table

    def save_klines(self, klines: list[Kline]) -> list[str] | None:
        table = self.client.get_table(table=self.table)
        error = self.client.insert_rows(
            table=table, rows=[kline.to_dict() for kline in klines]
        )

        if error:
            print(error)
            return

    def fetch_klines(self, end_time: datetime) -> list[list[Kline]]:
        result = []
        for i in range(2):
            kline = self.fetch_kline(end_time - timedelta(hours=i))
            result.append(kline)

        return result

    def fetch_kline(self, end_time: datetime) -> list[Kline]:
        start_time = end_time - timedelta(hours=1)
        query = f"""
            SELECT * FROM {self.table} WHERE open_time = ? LIMIT 1000
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(None, "TIMESTAMP", start_time),
                bigquery.ScalarQueryParameter(None, "TIMESTAMP", end_time),
            ]
        )

        query_job = self.client.query(query, job_config=job_config)
        rows = query_job.result()
        result = []
        for row in rows:
            tuple = row.values()
            result.append(Kline.from_tuple(tuple))

        return result
