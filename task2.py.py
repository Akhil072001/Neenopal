import pandas as pd
import mysql.connector

# Reading given CSV file from 'file_path' location
file_path = 'used_bikes.csv'
data = pd.read_csv(file_path)
dataDuplicate = data.copy()  # Make a copy of the DataFrame

memory_before = data.memory_usage(deep=True).sum()

# Converting all Integer Type Columns to String type columns
data['price'] = data['price'].astype(str)
data['kms_driven'] = data['kms_driven'].astype(str)
data['age'] = data['age'].astype(str)
data['power'] = data['power'].astype(str)

memory_after = data.memory_usage(deep=True).sum()
print("Memory Before:", memory_before/1024, "kB")
print("Memory After:", memory_after/1024, "kB")

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Suraj@2001",
    database="task2"
)

cursor = conn.cursor()

drop_table_query = '''
    DROP TABLE IF EXISTS data;
'''

cursor.execute(drop_table_query)
conn.commit()

create_table_query = '''
    CREATE TABLE IF NOT EXISTS data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        bike_name VARCHAR(255),
        price INT,
        city VARCHAR(255),
        kms_driven VARCHAR(255),
        owner VARCHAR(255),
        age INT, 
        power INT,
        brand VARCHAR(255)
    );
'''
cursor.execute(create_table_query)

# Convert DataFrame to a list of tuples for insertion
data_tuples = [tuple(row) for row in data.values]

insert_query = '''
    INSERT INTO data (bike_name, price, city, kms_driven, owner, age, power, brand)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

cursor.executemany(insert_query, data_tuples)
conn.commit()

cursor.close()
conn.close()

print("File dumped successfully!")