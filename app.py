# app.py
from flask import Flask, jsonify
import psycopg2
from psycopg2 import sql
import os

app = Flask(__name__)

# Database connection parameters
DATABASE_URL = os.getenv('DATABASE_URL')

# Connect to the database
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# Create a users table if it doesn't exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(80) NOT NULL
)
""")
conn.commit()

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/users')
def get_users():
    cursor.execute("SELECT id, name FROM users")
    users = cursor.fetchall()
    return jsonify([{'id': user[0], 'name': user[1]} for user in users])

@app.route('/add_user/<name>')
def add_user(name):
    cursor.execute(sql.SQL("INSERT INTO users (name) VALUES (%s) RETURNING id"), [name])
    conn.commit()
    user_id = cursor.fetchone()[0]
    return jsonify({'id': user_id, 'name': name})

if __name__ == '__main__':
    app.run(host='192.168.1.164', port=8080)