# on:  (hello-snowpark-git-py39) /Users/jsoliz/src/github/sfc-gh-jsoliz/hello-snowpark-git:hello-snowpark-git $
# run: python app_connect_to_snow.py --mode remote

from __future__ import annotations
from snowflake.snowpark.session import Session
from snowflake.snowpark import DataFrame
from snowflake.snowpark.functions import col
import json
import argparse



def hello_local(session: Session) -> DataFrame:
    df = session.table("products")
    return df.filter(col("ID") == 1)

def hello_remote(session: Session) -> DataFrame:
    df = session.table("git.test_data.customers")
    # df = df.groupBy("STATE").count()
    return df


# For local debugging
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="at CLI take local or remote for local testing or connecting to a snow instance")
    parser.add_argument("--mode", choices=["local", "remote"], help="Choose mode (local or remote)")
    args = parser.parse_args()

    if args.mode == 'local':
        # from snowflake.snowpark.mock.connection import MockServerConnection
        # session = Session(MockServerConnection())                      # LEGACY WAY PrPr way to Build Mock Session

        import init_local
        session = Session.builder.config('local_testing', True).create() # Better PuPr way to build mock sedssion 
        session = init_local.init(session)  # generate a fake products table
        print(hello_local(session).show())
    
    if args.mode == 'remote':
        session = Session.builder.configs(json.load(open("/Users/jsoliz/.creds/snowflake_connection.json"))).create()
        print(hello_remote(session).show())

