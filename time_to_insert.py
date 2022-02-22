import stat
import pandas as pd
import time
from timeit import timeit
from io import StringIO
import csv
import os
from psycopg2.extras import execute_values


class InsertTime:
    def __init__(self, df, engine, n_reps):
        self.df = df
        self.engine = engine
        self.n_reps = n_reps
        self.log = {}

    def change_engine(self, engine):
        self.engine = engine

    def time_to_insert(self, callable=None, chunksize=None):
        """Execute pd.to_sql with the given callable and chunksize n_reps times"""
        with self.engine.begin() as conn:
            # delete table
            if self.engine.dialect.has_table(
                connection=conn, table_name="test_table", schema="public"
            ):
                conn.execute(f'DROP TABLE "public"."test_table"')
            conn.close()

        with self.engine.begin() as conn:
            # with conn.begin():
            elapsed_time = timeit(
                "self.df.to_sql('test_table', conn, if_exists='replace', index=False, method=callable, chunksize=chunksize)",
                globals=locals(),
                number=self.n_reps,
                timer=time.process_time,
            )

            print(
                f"Averge elapsed time across {self.n_reps} runs: {elapsed_time/self.n_reps}"
            )
            return {
                "n_reps:": self.n_reps,
                "avg_rep_time": elapsed_time / self.n_reps,
            }

    def set_logged(self):
        with self.engine.begin() as conn:
            conn.execute("ALTER TABLE test_table SET LOGGED")

    @staticmethod
    def callable_1(table, conn, keys, data_iter):
        """Approximates the 'default' pd.to_sql behavior"""
        data = [dict(zip(keys, row)) for row in data_iter]
        conn.execute(table.table.insert(), data)

    @staticmethod
    def callable_1a(table, conn, keys, data_iter):
        """Approximates the 'default' pd.to_sql behavior"""
        # data = [zip(keys, row) for row in data_iter]
        dbapi_conn = conn.connection
        cols = ", ".join(keys)
        vals = ",".join(["%s"] * len(keys))

        with dbapi_conn.cursor() as cur:
            sql = f"INSERT INTO {table.name} ({cols}) VALUES ({vals})"
            cur.executemany(sql, data_iter)

    @staticmethod
    def callable_1b(table, conn, keys, data_iter):
        """Approximates the 'default' pd.to_sql behavior"""
        # https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
        dbapi_conn = conn.connection
        cols = ", ".join(keys)
        vals = ",".join(["%s"] * len(keys))
        vals = f"({vals})"

        with dbapi_conn.cursor() as cur:
            args_str = ",".join(
                cur.mogrify(vals, x).decode("utf-8") for x in tuple(data_iter)
            )
            cur.execute(f"INSERT INTO {table.name} ({cols}) VALUES " + args_str)

    @staticmethod
    def callable_1c(table, conn, keys, data_iter):
        """Insert using psycopg2 execute_values"""
        # https://www.psycopg.org/docs/extras.html#psycopg2.extras.execute_values
        # src https://github.com/psycopg/psycopg2/blob/1d3a89a0bba621dc1cc9b32db6d241bd2da85ad1/lib/extras.py#L1190
        dbapi_conn = conn.connection
        cols = ", ".join(keys)
        with dbapi_conn.cursor() as cur:
            execute_values(
                cur=cur,
                sql=f"INSERT INTO {table.name} ({cols}) VALUES %s",
                argslist=tuple(data_iter),
                page_size=100
            )

    @staticmethod
    def callable_2(table, conn, keys, data_iter):
        """COPY method w/saved file"""
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            # s_buf = StringIO()
            with open("tmp.csv", "w", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(data_iter)

            columns = ", ".join(f'"{k}"' for k in keys)
            table_name = f'"{table.schema}"."{table.name}"'
            sql = f"COPY {table.name} ({columns}) FROM STDIN WITH CSV;"
            cur.copy_expert(sql=sql, file=open("tmp.csv", "r"))

    @staticmethod
    def callable_3(table, conn, keys, data_iter):
        """COPY method w/buffer"""
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)
            columns = ", ".join(f'"{k}"' for k in keys)
            sql = f"COPY {table.name} ({columns}) FROM STDIN WITH CSV"
            cur.copy_expert(sql=sql, file=s_buf)

    @staticmethod
    def callable_4(table, conn, keys, data_iter):
        """Unlogged Method"""
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)
            columns = ", ".join(f'"{k}"' for k in keys)
            sql = f"ALTER TABLE test_table SET UNLOGGED; COPY {table.name} ({columns}) FROM STDIN WITH CSV"
            cur.copy_expert(sql=sql, file=s_buf)
            # cur.execute("ALTER TABLE test_table SET LOGGED")

    def run_comparison(self):
        """Compare the different ways to insert data"""
        self.log["default"] = self.time_to_insert(callable=None, chunksize=None)
        self.log["default_2"] = self.time_to_insert(callable=self.callable_1, chunksize=None)
        self.log["copy_savefile"] = self.time_to_insert(callable=self.callable_2)
        os.remove("tmp.csv")
        self.log["copy_buffer"] = self.time_to_insert(callable=self.callable_3)
        self.log["unlogged"] = self.time_to_insert(callable=self.callable_4)
        self.set_logged()

        self.log["execute_multi"] = self.time_to_insert(callable=self.callable_1a, chunksize=None)
        self.log["mogrify"] = self.time_to_insert(callable=self.callable_1b, chunksize=100)
        self.log["execute_values"] = self.time_to_insert(
             callable=self.callable_1c, chunksize=None
        )

        return self.log
