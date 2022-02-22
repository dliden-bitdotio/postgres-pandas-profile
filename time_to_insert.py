import pandas as pd
import time
from timeit import timeit
from io import StringIO
import csv
import os

class InsertTime:
    def __init__(self, df, engine, n_reps):
        self.df = df
        self.engine = engine
        self.n_reps = n_reps
        self.log = {}

    def time_to_insert(self, callable=None, chunksize=None):
        """Execute pd.to_sql with the given callable and chunksize n_reps times"""
        with self.engine.connect() as conn:
            # delete table
            if self.engine.dialect.has_table(
                connection=conn, table_name="test_table", schema="public"
            ):
                conn.execute(f'DROP TABLE "public"."test_table"')
            conn.close()

        with self.engine.connect() as conn:
            elapsed_time = timeit(
                "self.df.to_sql('test_table', conn, if_exists='replace', index=False, method=callable, chunksize=chunksize)",
                globals=locals(),
                number=self.n_reps,
                timer=time.process_time,
            )

            print(
                f"Averge elapsed time across {self.n_reps} runs: {elapsed_time/self.n_reps}"
            )
            return {"n_reps:":self.n_reps, "avg_rep_time": elapsed_time / self.n_reps}

    @staticmethod
    def callable_1(table, conn, keys, data_iter):
        """Approximates the 'default' pd.to_sql behavior"""
        data = [dict(zip(keys, row)) for row in data_iter]
        conn.execute(table.table.insert(), data)

    @staticmethod
    def callable_2(table, conn, keys, data_iter):
        """COPY method w/saved file"""
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            #s_buf = StringIO()
            with open('tmp.csv', 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(data_iter)

            columns = ", ".join(f'"{k}"' for k in keys)
            table_name = f'"{table.schema}"."{table.name}"'
            sql = f"COPY {table.name} ({columns}) FROM STDIN WITH CSV"
            cur.copy_expert(sql=sql, file=open("tmp.csv", "r"))
    
    @staticmethod
    def callable_3(table, conn, keys, data_iter):
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ", ".join(f'"{k}"' for k in keys)
            sql = f"COPY {table.name} ({columns}) FROM STDIN WITH CSV"

            cur.copy_expert(sql=sql, file=s_buf)
    
    def run_comparison(self):
        """Compare the different ways to insert data"""
        self.log["default"] = self.time_to_insert(callable=None, chunksize=None)
        self.log["default_2"] = self.time_to_insert(callable=self.callable_1, chunksize=None)
        self.log["copy_savefile"] = self.time_to_insert(callable=self.callable_2)
        os.remove("tmp.csv")
        self.log["copy_buffer"] = self.time_to_insert(callable=self.callable_3)
        return self.log
