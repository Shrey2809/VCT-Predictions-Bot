import sqlite3

# Connect to a new or existing SQLite database
# conn = sqlite3.connect('VCT_2024_963095570124251136.db')
conn = sqlite3.connect('VCT_2024_1042862967072501860.db')

# Create a cursor object to interact with the database
cursor = conn.cursor()

# SQL commands to create tables with auto-incremented primary key and adjusted user_id field
create_table_queries = [
    """
    CREATE TABLE DS_VCT_2024 (
        id                        INTEGER PRIMARY KEY AUTOINCREMENT,
        user_name                 VARCHAR(50),
        user_id                   BIGINT,
        kickoff_americas          INT DEFAULT 0,
        kickoff_emea              INT DEFAULT 0,
        kickoff_china             INT DEFAULT 0,
        kickoff_pacific           INT DEFAULT 0,
        il1_americas              INT DEFAULT 0,
        il1_emea                  INT DEFAULT 0,
        il1_china                 INT DEFAULT 0,
        il1_pacific               INT DEFAULT 0,
        il2_americas              INT DEFAULT 0,
        il2_emea                  INT DEFAULT 0,
        il2_china                 INT DEFAULT 0,
        il2_pacific               INT DEFAULT 0,
        masters_shanghai_groups   INT DEFAULT 0,
        masters_shanghai_playoffs INT DEFAULT 0,
        masters_madrid_groups     INT DEFAULT 0,
        masters_madrid_playoffs   INT DEFAULT 0,
        champions_korea_groups    INT DEFAULT 0,
        champions_korea_playoffs  INT DEFAULT 0
    );"""
]


# Execute each create table query
for query in create_table_queries:
    cursor.execute(query)

# Commit changes to the database
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
