import cProfile
import csv
import json
import os
import time
from importlib import reload
from io import StringIO
from timeit import timeit

import pandas as pd
from sqlalchemy import create_engine

from time_to_insert import InsertTime

N_REPS = 5

if __name__ == "__main__":
    # Connection Details
    conn_dict = {
        "host": "localhost",
        "port": "5432",
        "database": "pd_test",
        "user": "danielliden",
        "password": "",
    }

    # load data
    dfs = {f"df1e{i}":pd.read_csv(f"./data/sims/sim_data_{int(10**i)}.csv") for i in range(2, 6)}
    # create engine
    engine = create_engine(
        f"postgresql+psycopg2://{conn_dict['user']}:{conn_dict['password']}@{conn_dict['host']}:{conn_dict['port']}/{conn_dict['database']}"
    )

    # initialize testing classes
    classDict = {k:InsertTime(v, engine=engine, n_reps=N_REPS) for k,v in (dfs.items())}

    # Iterate through dataframes to compare runs
    out = []
    for k in classDict.keys():
        df = pd.DataFrame.from_dict(classDict[k].run_comparison(), orient="index")
        df["test"] = k
        out.append(df)
    
    # concatenate results and save to CSV
    out = pd.concat(out, axis=0)
    out.to_csv("./data/prelim_02242022.csv")

    print(out)

