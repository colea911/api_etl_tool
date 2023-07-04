# A command to easily run SQL queries out of terminal
# Example Usage - python scripts/database_query.py "SELECT * FROM algorand_transactions"

import argparse
from sqlalchemy import create_engine, text

def execute_query(query):
    engine = create_engine('sqlite:///bitpool.db')
    with engine.connect() as connection:
        stmt = text(query)
        result = connection.execute(stmt)
        rows = result.fetchall()
        for row in rows:
            print(row)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute SQL query on the database")
    parser.add_argument("query", help="SQL query to execute")
    args = parser.parse_args()
    
    query = args.query
    execute_query(query)

