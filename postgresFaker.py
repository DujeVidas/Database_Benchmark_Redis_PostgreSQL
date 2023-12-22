"""
This script utilizes the Faker library to generate fictitious user data. 
It connects to a PostgreSQL database and 
populates the "users" table with 1000 entries 
containing randomly generated name, address, phone number, date of birth, email, and credit card information. 
After executing the inserts, it commits the changes and closes the database connection.
"""


import psycopg2
from faker import Faker

fake = Faker()


conn = psycopg2.connect(
    dbname='mydatabase',
    user='dujevidas',
    password='DVidas123',
    host='postgres',
    port='5432'
)
cur = conn.cursor()


for _ in range(1000):
    name = fake.name()[:100]
    address = fake.address()
    phone_number = fake.phone_number()[:20]
    date_of_birth = fake.date_of_birth()
    email = fake.email()[:100]
    credit_card = fake.credit_card_full()[:100]
    
    cur.execute("INSERT INTO users (name, address, phone_number, date_of_birth, email, credit_card) VALUES (%s, %s, %s, %s, %s, %s)", (name, address, phone_number, date_of_birth, email, credit_card))



conn.commit()
cur.close()
conn.close()
