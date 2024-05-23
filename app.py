# app.py

from flask import Flask, jsonify, request, redirect, url_for, session
import psycopg2
from psycopg2 import sql
import os
import requests
from datetime import datetime

app = Flask(__name__)
app.secret_key = 'your_secret_key'

# Database connection parameters
DATABASE_URL = os.getenv('DATABASE_URL')
CANVAS_CLIENT_ID = os.getenv('CANVAS_CLIENT_ID')
CANVAS_CLIENT_SECRET = os.getenv('CANVAS_CLIENT_SECRET')
CANVAS_REDIRECT_URI = os.getenv('CANVAS_REDIRECT_URI')
CANVAS_AUTHORIZATION_URL = os.getenv('CANVAS_AUTHORIZATION_URL')
CANVAS_TOKEN_URL = os.getenv('CANVAS_TOKEN_URL')

# Connect to the database
def get_db_connection():
    conn = psycopg2.connect(DATABASE_URL)
    return conn

# Initialize database and create tables if they don't exist
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS owners (
        id SERIAL PRIMARY KEY,
        full_name VARCHAR(100) NOT NULL,
        display_name VARCHAR(100) NOT NULL,
        email VARCHAR(100) NOT NULL,
        pws_student_number VARCHAR(20)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS media (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255),
        description TEXT,
        duration NUMERIC,
        created_at TIMESTAMP,
        thumbnail_url VARCHAR(255),
        transcoding_status VARCHAR(50),
        size BIGINT,
        source VARCHAR(50),
        owner_id INTEGER REFERENCES owners(id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS downloads (
        id SERIAL PRIMARY KEY,
        media_id INTEGER REFERENCES media(id),
        download_start_timestamp TIMESTAMP,
        download_complete_timestamp TIMESTAMP
    )
    """)

    conn.commit()
    cursor.close()
    conn.close()

init_db()

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/login')
def login():
    canvas_authorize_url = (
        f"{CANVAS_AUTHORIZATION_URL}?client_id={CANVAS_CLIENT_ID}"
        f"&response_type=code&redirect_uri={CANVAS_REDIRECT_URI}&scope=canvas_api"
    )
    return redirect(canvas_authorize_url)

@app.route('/oauth2/callback')
def oauth2_callback():
    code = request.args.get('code')
    token_response = requests.post(CANVAS_TOKEN_URL, data={
        'grant_type': 'authorization_code',
        'client_id': CANVAS_CLIENT_ID,
        'client_secret': CANVAS_CLIENT_SECRET,
        'redirect_uri': CANVAS_REDIRECT_URI,
        'code': code
    })

    token_json = token_response.json()
    session['access_token'] = token_json['access_token']
    return redirect(url_for('fetch_and_insert'))

def insert_owner(cursor, owner):
    cursor.execute(sql.SQL("""
        INSERT INTO owners (id, full_name, display_name, email, pws_student_number)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
        RETURNING id
    """), [owner['id'], owner['full_name'], owner['display_name'], owner['email'], owner.get('pws_student_number')])

    owner_id = cursor.fetchone()
    if owner_id is None:
        cursor.execute(sql.SQL("SELECT id FROM owners WHERE id = %s"), [owner['id']])
        owner_id = cursor.fetchone()[0]
    else:
        owner_id = owner_id[0]
    return owner_id

def insert_media(cursor, item, owner_id):
    cursor.execute(sql.SQL("""
        INSERT INTO media (id, title, description, duration, created_at, thumbnail_url, transcoding_status, size, source, owner_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """), [item['id'], item['title'], item['description'], item['duration'], item['created_at'], item['thumbnail_url'], item['transcoding_status'], item['size'], item['source'], owner_id])

@app.route('/fetch_and_insert', methods=['GET', 'POST'])
def fetch_and_insert():
    access_token = session.get('access_token')
    if not access_token:
        return redirect(url_for('login'))

    headers = {'Authorization': f'Bearer {access_token}'}
    page = 1
    while True:
        response = requests.get(
            f'https://teneo.instructuremedia.com/api/public/v1/media/search?page={page}&per_page=50&start_date=2024-01-01',
            headers=headers
        )
        data = response.json()
        if 'media' not in data or not data['media']:
            break
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for item in data['media']:
            owner = item['owner']
            owner_id = insert_owner(cursor, owner)
            insert_media(cursor, item, owner_id)
            
        conn.commit()
        cursor.close()
        conn.close()

        # Check if there are more pages
        if page >= data['meta']['last_page']:
            break
        page += 1
    
    return jsonify({'status': 'success'}), 201

@app.route('/media', methods=['GET'])
def get_media():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT m.id, m.title, m.description, m.duration, m.created_at, m.thumbnail_url, m.transcoding_status, m.size, m.source, o.id, o.full_name, o.display_name, o.email, o.pws_student_number
        FROM media m
        JOIN owners o ON m.owner_id = o.id
    """)
    media = cursor.fetchall()
    cursor.close()
    conn.close()

    media_list = [{
        'id': m[0],
        'title': m[1],
        'description': m[2],
        'duration': m[3],
        'created_at': m[4],
        'thumbnail_url': m[5],
        'transcoding_status': m[6],
        'size': m[7],
        'source': m[8],
        'owner': {
            'id': m[9],
            'full_name': m[10],
            'display_name': m[11],
            'email': m[12],
            'pws_student_number': m[13]
        }
    } for m in media]

    return jsonify(media_list)

@app.route('/download_media', methods=['POST'])
def download_media():
    data = request.json
    media_id = data['media_id']
    download_start_timestamp = datetime.utcnow()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(sql.SQL("""
        INSERT INTO downloads (media_id, download_start_timestamp)
        VALUES (%s, %s)
        RETURNING id
    """), [media_id, download_start_timestamp])

    download_id = cursor.fetchone()[0]

    # Simulate download process
    # In a real scenario, you would add your download logic here.
    download_complete_timestamp = datetime.utcnow()

    cursor.execute(sql.SQL("""
        UPDATE downloads
        SET download_complete_timestamp = %s
        WHERE id = %s
    """), [download_complete_timestamp, download_id])

    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({'status': 'success'}), 201

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
