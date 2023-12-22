"""
This script utilizes the Faker library to generate fictitious user data.. 
It iterates 1000 times, creating a unique key for each entry and assigning a JSON object as the corresponding value. 
The JSON object includes fabricated details like name, address, phone number, date of birth, email, and credit card information. 
These key-value pairs are set in the Redis database.
"""

from faker import Faker
import redis

import json

fake = Faker()


r = redis.StrictRedis(host='redis', port=6379, db=0)

for _ in range(1000):
    key = fake.uuid4()
    value = {
        'name': fake.name(),
        'address': fake.address(),
        'phone_number': fake.phone_number(),
        'date_of_birth': fake.date_of_birth().strftime('%Y-%m-%d'),
        'email': fake.email(),
        'credit_card': fake.credit_card_full(),
    }
    r.set(key, json.dumps(value))

