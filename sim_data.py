import numpy as np
import pandas as pd
import os
import json

from mimesis.enums import Gender
from mimesis.schema import Field, Schema
from mimesis.locales import Locale
from mimesis import builtins, random


def gen_examples(nrows, seed):
    _ = Field(locale=Locale.EN, seed=seed)
    schema = Schema(
        schema=lambda: {
            "number": _("increment"),
            "uid": _("uuid"),
            "name": _("text.word"),
            "version": _("version", pre_release=True),
            "timestamp": _("timestamp", posix=False),
            "owner": {
                "email": _("person.email", domains=["test.com"], key=str.lower),
                "token": _("token_hex"),
                "creator": _("full_name", gender=Gender.FEMALE),
            },
            "published": _("text.answer"),
            "revenue": _("random.uniform", precision=0, a=2, b=1e5),
        }
    )
    x = pd.DataFrame.from_dict(schema.create(iterations=nrows))
    x["owner"] = x["owner"].apply(json.dumps)

    return x  # .memory_usage().sum()/1024/1024/1024 # GB


def save_data(df, path, name):
    if not os.path.exists(path):
        os.makedirs(path)
    df.to_csv(path + "/" + name, index=False)


if __name__ == "__main__":
    sizes = [int(a) for a in [1e2, 1e3, 1e4, 1e5, 1e6, 1e7]]
    seed = 42
    path = "./data/sims/"

    # Generate CSVs for use in the analysis.

    for size in sizes:
        df = gen_examples(size, seed)
        save_data(df, path, "sim_data_" + str(size) + ".csv")
