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
