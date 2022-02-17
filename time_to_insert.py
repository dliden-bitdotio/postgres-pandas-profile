import pandas as pd
import time
from timeit import timeit


def time_to_insert(df, engine, n_reps, callable=None, chunksize=None):
    with engine.connect() as conn:
        # delete table
        if engine.dialect.has_table(
            connection=conn, table_name="test_table", schema="public"
        ):
            conn.execute(f'DROP TABLE "public"."test_table"')
        conn.close()

    with engine.connect() as conn:
        elapsed_time = timeit(
            "df.to_sql('test_table', conn, if_exists='replace', index=False, method=callable, chunksize=chunksize)",
            globals=locals(),
            number=n_reps,
            timer=time.process_time,
        )

        print(
            f"Averge elapsed time across {n_reps} runs: {elapsed_time/n_reps}"
        )


class InsertTime:
    def __init__(self, df, engine, n_reps):
        self.df = df
        self.engine = engine
        self.n_reps = n_reps

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
                "df.to_sql('test_table', conn, if_exists='replace', index=False, method=callable, chunksize=chunksize)",
                globals=locals(),
                number=self.n_reps,
                timer=time.process_time,
            )

            print(
                f"Averge elapsed time across {self.n_reps} runs: {elapsed_time/self.n_reps}"
            )

    def callable_1(table, conn, keys, data_iter):
        """Approximates the 'default' pd.to_sql behavior"""
        data = [dict(zip(keys, row)) for row in data_iter]
        conn.execute(table.table.insert(), data)

    def callable_2(table, conn, keys, data_iter):
        """"""