import sqlite3

# Connect to a new or existing SQLite database
conn = sqlite3.connect('VCT_2024_963095570124251136.db')
# conn = sqlite3.connect('VCT_2024_1042862967072501860.db')

# Create a cursor object to interact with the database
cursor = conn.cursor()

# SQL commands to create tables with auto-incremented primary key and adjusted user_id field
create_table_queries = [
    '''
    Create TABLE DS_2024_FILES_PROCESSED (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name VARCHAR(50),
        created_date_time   DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    ''',
    '''
    CREATE TABLE DS_2024_KICKOFF (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        user_name VARCHAR(50),
        user_id   BIGINT,
        americas  INT DEFAULT 0,
        emea      INT DEFAULT 0,
        china     INT DEFAULT 0,
        pacific   INT DEFAULT 0
    );
    ''',
    '''
    CREATE TABLE DS_2024_IL1 (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        user_name VARCHAR(50),
        user_id   BIGINT,
        americas  INT DEFAULT 0,
        emea      INT DEFAULT 0,
        china     INT DEFAULT 0,
        pacific   INT DEFAULT 0
    );
    ''',
    '''
    CREATE TABLE DS_2024_IL2 (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        user_name VARCHAR(50),
        user_id   BIGINT DEFAULT 0,
        americas  INT DEFAULT 0,
        emea      INT DEFAULT 0,
        china     INT DEFAULT 0,
        pacific   INT DEFAULT 0
    );
    ''',
    '''
    CREATE TABLE DS_2024_MASTERS (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        user_name         VARCHAR(50),
        user_id           BIGINT,
        shanghai_groups   INT DEFAULT 0,
        shanghai_playoffs INT DEFAULT 0,
        madrid_groups     INT DEFAULT 0,
        madrid_playoffs   INT DEFAULT 0
    );
    ''',
    '''
    CREATE TABLE DS_2024_CHAMPIONS (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        user_name       VARCHAR(50),
        user_id         BIGINT,
        korea_groups    INT DEFAULT 0,
        korea_playoffs  INT DEFAULT 0
    );
    '''
]


# Execute each create table query
for query in create_table_queries:
    cursor.execute(query)

# Commit changes to the database
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
