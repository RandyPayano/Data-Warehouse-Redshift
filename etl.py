import configparser
import psycopg2
from lib.create_aws_resources import create_aws_resources
from lib.delete_aws_resources import delete_aws_resources
import pandas as pd
from lib.sql_queries import sql_queries



def drop_tables(cur, conn):
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("Tables dropped")
    print("")

def create_tables(cur, conn):
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("Tables created")   
    print("")
    
def load_staging_tables(cur, conn):
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print("Staging tables loaded.")
    print("")

def insert_tables(cur, conn):
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print("Star schema tables loaded.")
    print("")
def main():
    # creating resources
    create_aws_resources()
    
    # load configuration file
    config = configparser.ConfigParser()
    config.read('func.cfg')
    
    # connect to database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # load sql queries after configuration file have changed
    global create_table_queries, drop_table_queries, copy_table_queries, insert_table_queries
    create_table_queries, drop_table_queries, copy_table_queries, insert_table_queries = sql_queries()

    # drop and create tables 
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    # loading staging tables into cluster
    load_staging_tables(cur, conn)
    
    # inserting into star schemas tables
    insert_tables(cur, conn)
    
    # validation
    
    question = input("To delete all AWS resources press: Y | To query database press: T ---> :").lower()
    
    if question == "y":
        # closing connection 
        conn.close()
        # deleting resources 
        delete_aws_resources()
        
    if question == "t":
        print("")
        try:
            cur.execute("""select fs.location, count(distinct(users.user_id)) as count from dim_users as users join fact_songplay as fs on users.user_id = fs.user_id Group By fs.location ORDER BY count Desc LIMIT 5"""
          )
            df = pd.DataFrame(cur.fetchone()).T  
            print(df.values)
            print("")
            # close connection
            conn.close()
            # deleting resources 
            delete_aws_resources()
            
        except:
            # close connection
            conn.close()
            # deleting resources 
            delete_aws_resources()
    else:
        print("Wrong answer")
        # close connection
        conn.close()
        # deleting resources 
        delete_aws_resources()
        
if __name__ == "__main__":
    main()