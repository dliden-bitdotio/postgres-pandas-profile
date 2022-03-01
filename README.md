# Populate a PosgreSQL Database Table Faster with Custom Pandas.DataFrame.to_sql() Methods


![fast_insert_2 drawio (4)](https://user-images.githubusercontent.com/84750618/156069208-b21c75d0-faa1-4e92-bcb7-05fb96af36ce.png)

This repository accompanies the article [Populating a PostgreSQL Table with Pandas is Slow. Here's How to Speed it Up.](https://innerjoin.bit.io/populating-a-postgresql-table-with-pandas-is-slow-7bc63e9c88dc).

## Run the Experiment Yourself

1. Connect to a PostgreSQL Database. We used a local database. Details on how to do this can be found in the [PostgreSQL documentation](https://www.postgresql.org/docs/14/tutorial-createdb.html).
2. Generate the synthetic data. This process is defined in the [sim_data.py](sim_data.py) file, which uses the mimesis package to generate data according to a pre-defined schema. You can modify the schema and/or the file sizes as needed in this file. Running this file from the terminal (`python sim_data.py`) will generate CSV files with synthetic data. One file will be generated for each number of rows specified in the `sim_data.py` file.
3. Define custom `DataFrame.to_sql` `method` callables in the [time_to_insert.py](time_to_insert.py) file. This module defines a class, InsertTime, that takes a DataFrame, a database engine, and a number of repetitions as arguments. The class includes a number of static methods for different callables to the `DataFrame.to_sql` `method` argument. It also defines a `run_comparison` method specifying which callables should be used; any intermediate or cleanup steps (such as deleting temporary files) that should be taken, etc., and returns a dictionary log with the insert times. 
4. Run the comparison experiment with [experiment.py](experiment.py). This sets up the database connection engine, runs the experiment on each of the CSVs generated in (1) (though the logic will need to be tweaked for different naming schemes and different synthetic data structures), and processes and saves the logs as a new CSV.
5. Analyze the results however you want! See [the artcicle](https://innerjoin.bit.io/populating-a-postgresql-table-with-pandas-is-slow-7bc63e9c88dc) for inspiration.
