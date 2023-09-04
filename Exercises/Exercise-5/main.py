import csv
import os
import psycopg2


def main():
    # PostgreSQL connection parameters.
    params = {"host":"postgres",
    "database":"postgres",
    "user":"postgres",
    "password":"postgres"
    }
    
    # your code here
    directory_path = './data'
    create_table_stmt = """
    DROP TABLE IF EXISTS products;
    DROP TABLE IF EXISTS accounts;
    DROP TABLE IF EXISTS transactions;
    CREATE TABLE IF NOT EXISTS products (product_id smallint, product_code text, product_description text);
    CREATE TABLE IF NOT EXISTS accounts (customer_id smallint, first_name text, last_name text, address_1 text, address_2 text, city text, state text, zip_code integer, join_date text);
    CREATE TABLE IF NOT EXISTS transactions (transaction_id text, transaction_date text, product_id smallint, product_code text, product_description text, quantity smallint, account_id smallint);"""

    # Connect to the PostgreSQL server.
    conn = psycopg2.connect(**params)

    # Create a new cursor.
    cur = conn.cursor()
    # Execute the CREATE TABLE statement.
    cur.execute(create_table_stmt)
    # Commit changes
    conn.commit()

    # Define the directory where the CSV files are located.
    directory_path = './data'

    # Loop over all CSV files in the directory.
    for filename in os.listdir(directory_path):
        if filename.endswith('.csv'):
            # Open the CSV file and create a csv.reader object.
            with open(os.path.join(directory_path, filename), 'r') as f:
                reader = csv.reader(f)

                # Get the header (first row of the CSV file).
                # next 함수는 행을 읽어오면서 위치를 다음 행으로 이동시킨다.
                header = next(reader)
                table_name = filename.split(".")[0]
                # Insert data from each row in the CSV file into the table.
                for row in reader:
                    formatted_row = ["'" + item + "'" if item.replace('.','').replace('/','').isdigit() == False else item for item in row]
                    cur.execute(f"INSERT INTO {table_name} VALUES ({', '.join(formatted_row)});")
                
            # Commit changes after all rows have been inserted into table    
            conn.commit()

    # Close communication with database
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()



