import pandas as pd

# Get points and rank for specific user for a region
def get_league_sum(conn, league, username=None, user_id=None):
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    if league in ["china", "pacific", "americas", "emea"]:
        query = f"""
            SELECT user_name, total_{league}, rank_{league}
            FROM (
                SELECT user_name, user_id, total_{league}, rank_{league},
                    ROW_NUMBER() OVER (ORDER BY total_{league} DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM({league}), 0) AS total_{league},
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM({league}), 0) DESC) AS rank_{league}
                    FROM (
                        SELECT user_name, user_id, {league} FROM DS_2024_KICKOFF
                        UNION ALL
                        SELECT user_name, user_id, {league} FROM DS_2024_IL1
                        UNION ALL
                        SELECT user_name, user_id, {league} FROM DS_2024_IL2
                    ) AS all_{league}
                    GROUP BY user_name, user_id
                ) AS ua
            ) AS ranked_users
            WHERE user_name = '{username}' or user_id = {user_id};
        """
    elif league in ["madrid", "shanghai"]:
        query = f"""
            SELECT user_name, total_{league}, rank_{league}
            FROM (
                SELECT user_name, user_id, total_{league}, rank_{league},
                    ROW_NUMBER() OVER (ORDER BY total_{league} DESC) AS row_num
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM({league}_groups + {league}_playoffs), 0) AS total_{league},
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM({league}_groups + {league}_playoffs), 0) DESC) AS rank_{league}
                    FROM (
                        SELECT user_name, user_id, {league}_groups, {league}_playoffs FROM DS_2024_MASTERS
                    ) AS all_{league}
                    GROUP BY user_name, user_id
                ) AS ua
            ) AS ranked_users
            WHERE user_name = '{username}' or user_id = {user_id};
        """
    elif league in ["korea"]:
        query = f"""
           SELECT user_name, total_{league}, rank_{league}
            FROM (
                SELECT user_name, user_id, total_{league}, rank_{league},
                    ROW_NUMBER() OVER (ORDER BY total_{league} DESC) AS row_num
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM({league}_groups + {league}_playoffs), 0) AS total_{league},
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM({league}_groups + {league}_playoffs), 0) DESC) AS rank_{league}
                    FROM (
                        SELECT user_name, user_id, {league}_groups, {league}_playoffs FROM DS_2024_CHAMPIONS
                    ) AS all_{league}
                    GROUP BY user_name, user_id
                ) AS ua
            ) AS ranked_users
            WHERE user_name = '{username}' or user_id = {user_id};
        """
    
    # Execute the query with the provided username or user_id
    cursor.execute(query)
    
    # Fetch the result
    result = cursor.fetchall()
    
    # Close the cursor and the connection
    cursor.close()
    conn.close()
    
    # Return the result as a DataFrame
    df = pd.DataFrame(result, columns=['user_name', f'total_{league}', f'rank_{league}'])
    df = df.rename(columns={f'total_{league}': 'Points', f'rank_{league}': 'Rank'})
    return df

# Get points and rank for a specific user for a region + type
def get_specific_sum(conn, league, type, username=None, user_id=None):
    cursor = conn.cursor()

    if type in ["il1", "il2", "kickoff"]:
        query = f"""
            SELECT user_name, user_id, total_sum as total_{league}_{type}, dense_rank as rank_{league}_{type}
            FROM (
                SELECT user_id, user_name, total_sum, dense_rank,
                    ROW_NUMBER() OVER (ORDER BY total_sum DESC) AS row_num
                FROM (
                    SELECT user_id, user_name, total_sum, dense_rank
                    FROM (
                        SELECT user_id, user_name, total_sum,
                            ROW_NUMBER() OVER (ORDER BY total_sum DESC) AS dense_rank
                        FROM (
                            SELECT user_id, user_name, SUM({league}) AS total_sum
                            FROM DS_2024_{type.upper()}
                            GROUP BY user_id, user_name
                        ) AS sums
                    ) AS ranks
                ) AS ua
            ) AS ranked_users
            WHERE user_id = {user_id} or user_name = "{username}"
        """
    elif type in ["masters"]:
        query = f"""
            SELECT user_name, user_id, total_sum as total_{league}_{type}, dense_rank as rank_{league}_{type}
            FROM (
                SELECT user_id, user_name, total_sum, dense_rank,
                    ROW_NUMBER() OVER (ORDER BY dense_rank ASC) AS row_num
                FROM (
                    SELECT user_id, user_name, total_sum, dense_rank
                    FROM (
                        SELECT user_id, user_name, total_sum,
                            ROW_NUMBER() OVER (ORDER BY total_sum DESC) AS dense_rank
                        FROM (
                            SELECT user_id, user_name, SUM({league}_groups + {league}_playoffs) AS total_sum
                            FROM DS_2024_MASTERS
                            GROUP BY user_id, user_name
                        ) AS sums
                    ) AS ranks
                ) AS ua
            ) AS ranked_users
            WHERE user_id = {user_id} or user_name = "{username}"
        """
    elif type in ["champions"]:
        query = f"""
            SELECT user_name, user_id, total_sum as total_{league}_{type}, dense_rank as rank_{league}_{type}
            FROM (
                SELECT user_id, user_name, total_sum, dense_rank,
                    ROW_NUMBER() OVER (ORDER BY dense_rank ASC) AS row_num
                FROM (
                    SELECT user_id, user_name, total_sum, dense_rank
                    FROM (
                        SELECT user_id, user_name, total_sum,
                            ROW_NUMBER() OVER (ORDER BY total_sum DESC) AS dense_rank
                        FROM (
                            SELECT user_id, user_name, SUM({league}_groups + {league}_playoffs) AS total_sum
                            FROM DS_2024_CHAMPIONS
                            GROUP BY user_id, user_name
                        ) AS sums
                    ) AS ranks
                ) AS ua
            ) AS ranked_users
            WHERE user_id = {user_id} or user_name = "{username}"
        """
    
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    
    df = pd.DataFrame(result, columns=['user_id', 'user_name', f'total_{league}_{type}', f'rank_{league}_{type}'])
    df = df.rename(columns={f'total_{league}_{type}': 'Points', f'rank_{league}_{type}': 'Rank'})
    return df

# Get points and rank for a specific user for a type
def get_type_sum(conn, type, username=None, user_id=None):
    cursor = conn.cursor()

    if type in ["il1", "il2", "kickoff"]:
        query = f"""
            SELECT 
                user_name,
                user_id,
                total_sum as total_{type},
                rank_{type}
            FROM (
                SELECT 
                    user_id,
                    user_name,
                    total_sum,
                    ROW_NUMBER() OVER (ORDER BY total_sum DESC) AS rank_{type}
                FROM (
                    SELECT 
                        user_id,
                        user_name,
                        SUM(emea + china + pacific + americas) AS total_sum,
                        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY emea + china + pacific + americas DESC) AS rank_{type}
                    FROM DS_2024_{type.upper()}
                    GROUP BY user_id, user_name
                ) AS sums
            ) AS ranked_users
            WHERE user_id = {user_id} or user_name = "{username}"
        """
    elif type in ["masters"]:
        query = f"""
            SELECT 
                user_name,
                user_id,
                total_sum as total_{type},
                rank_{type}
            FROM (
                SELECT 
                    user_id,
                    user_name,
                    total_sum,
                    ROW_NUMBER() OVER (ORDER BY total_sum DESC) AS rank_{type}
                FROM (
                    SELECT 
                        user_id,
                        user_name,
                        SUM(shanghai_groups + shanghai_playoffs + madrid_groups + madrid_playoffs) AS total_sum
                    FROM DS_2024_MASTERS
                    GROUP BY user_id, user_name
                ) AS sums
            ) AS ranked_users
            WHERE user_id = {user_id} or user_name = "{username}"
        """
    elif type in ["champions"]:
        query = f"""
            SELECT 
                user_name,
                user_id,
                total_sum as total_{type},
                rank_{type}
            FROM (
                SELECT 
                    user_id,
                    user_name,
                    total_sum,
                    ROW_NUMBER() OVER (ORDER BY total_sum DESC) AS rank_{type}
                FROM (
                    SELECT 
                        user_id,
                        user_name,
                        SUM(korea_groups + korea_playoffs) AS total_sum
                    FROM DS_2024_CHAMPIONS
                    GROUP BY user_id, user_name
                ) AS sums
            ) AS ranked_users
            WHERE user_id = {user_id} or user_name = "{username}"
        """
    
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    df = pd.DataFrame(result, columns=['user_id', 'user_name', f'total_{type}', f'rank_{type}'])
    df = df.rename(columns={f'total_{type}': 'Points', f'rank_{type}': 'Rank'})
    return df

# Get points and rank for a specific user for all year long
def get_all_sum(conn, username=None,user_id=None):
    cursor = conn.cursor()

    query = f"""
        SELECT 
            user_name,
            user_id,
            overall_total,
            dense_rank
        FROM (
            SELECT 
                user_name,
                user_id,
                overall_total,
                ROW_NUMBER() OVER (ORDER BY overall_total DESC) AS dense_rank
            FROM (
                SELECT 
                    user_name, 
                    user_id, 
                    SUM(total_sum) AS overall_total
                FROM (
                    SELECT 
                        user_name, 
                        user_id, 
                        americas + emea + china + pacific AS total_sum
                    FROM DS_2024_KICKOFF

                    UNION ALL

                    SELECT 
                        user_name, 
                        user_id, 
                        americas + emea + china + pacific AS total_sum
                    FROM DS_2024_IL1

                    UNION ALL

                    SELECT 
                        user_name, 
                        user_id, 
                        americas + emea + china + pacific AS total_sum
                    FROM DS_2024_IL2

                    UNION ALL

                    SELECT 
                        user_name, 
                        user_id, 
                        shanghai_groups + shanghai_playoffs + madrid_groups + madrid_playoffs AS total_sum
                    FROM DS_2024_MASTERS

                    UNION ALL

                    SELECT 
                        user_name, 
                        user_id, 
                        korea_groups + korea_playoffs AS total_sum
                    FROM DS_2024_CHAMPIONS
                ) AS all_tables
                GROUP BY user_name, user_id
            ) AS sums
        ) AS ranked_users
        WHERE user_id = {user_id} or user_name = "{username}"
    """

    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()

    df = pd.DataFrame(result, columns=['user_name', 'user_id', 'overall_total', 'dense_rank'])
    df = df.rename(columns={'overall_total': 'Points', 'dense_rank': 'Rank'})

    return df

#---------------------------------------------------------------------------------------------
# Leaderboard Functions
# Get leaderboard for top 10 users for a region
def get_league_leaderboard(conn, league, username=None, user_id=None):
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    if league in ["china", "pacific", "americas", "emea"]:
        query = f"""
            SELECT user_name, total_{league}, rank_{league}
            FROM (
                SELECT user_name, total_{league}, rank_{league},
                    ROW_NUMBER() OVER (ORDER BY total_{league} DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM({league}), 0) AS total_{league},
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM({league}), 0) DESC) AS rank_{league}
                    FROM (
                        SELECT user_name, user_id, {league} FROM DS_2024_KICKOFF
                        UNION ALL
                        SELECT user_name, user_id, {league} FROM DS_2024_IL1
                        UNION ALL
                        SELECT user_name, user_id, {league} FROM DS_2024_IL2
                    ) AS all_{league}
                    GROUP BY user_name, user_id
                ) AS ua
            ) AS ranked_users
            WHERE overall_rank <= 10; -- Filter for top 10 ranked users

        """
    elif league in ["madrid", "shanghai"]:
        query = f"""
            SELECT user_name, total_{league}, rank_{league}
            FROM (
                SELECT user_name, user_id, total_{league}, rank_{league},
                    ROW_NUMBER() OVER (ORDER BY total_{league} DESC) AS row_num
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM({league}_groups + {league}_playoffs), 0) AS total_{league},
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM({league}_groups + {league}_playoffs), 0) DESC) AS rank_{league}
                    FROM (
                        SELECT user_name, user_id, {league}_groups, {league}_playoffs FROM DS_2024_MASTERS
                    ) AS all_{league}
                    GROUP BY user_name, user_id
                ) AS ua
            ) AS ranked_users
            WHERE row_num <= 10; -- Filter for the first 10 rows based on rankings
        """
    elif league in ["korea"]:
        query = f"""
            SELECT user_name, total_{league}, rank_{league}
            FROM (
                SELECT user_name, user_id, total_{league}, rank_{league},
                    ROW_NUMBER() OVER (ORDER BY total_{league} DESC) AS row_num
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM({league}_groups + {league}_playoffs), 0) AS total_{league},
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM({league}_groups + {league}_playoffs), 0) DESC) AS rank_{league}
                    FROM (
                        SELECT user_name, user_id, {league}_groups, {league}_playoffs FROM DS_2024_CHAMPIONS
                    ) AS all_{league}
                    GROUP BY user_name, user_id
                ) AS ua
            ) AS ranked_users
            WHERE row_num <= 10; -- Filter for the first 10 rows based on rankings
        """
    
    # Execute the query with the provided username or user_id
    cursor.execute(query)
    
    # Fetch the result
    result = cursor.fetchall()
    
    # Close the cursor and the connection
    cursor.close()
    conn.close()
    
    # Return the result as a DataFrame
    df = pd.DataFrame(result, columns=['user_name', f'total_{league}', f'rank_{league}'])
    df = df.rename(columns={'user_name' : 'Username', f'total_{league}': 'Points', f'rank_{league}': 'Rank'})
    df = df.head(10)
    return df

# Get points and rank for a specific user for a region + type
def get_specific_leaderboard(conn, league, type, username=None, user_id=None):
    cursor = conn.cursor()

    if type in ["il1", "il2", "kickoff"]:
        query = f"""
            SELECT user_name, total_sum as total_{league}_{type}, dense_rank as rank_{league}_{type}
            FROM (
                SELECT user_id, user_name, total_sum, dense_rank,
                    ROW_NUMBER() OVER (ORDER BY total_sum DESC) AS row_num
                FROM (
                    SELECT user_id, user_name, total_sum, dense_rank
                    FROM (
                        SELECT user_id, user_name, total_sum,
                            ROW_NUMBER() OVER (ORDER BY total_sum DESC) AS dense_rank
                        FROM (
                            SELECT user_id, user_name, SUM({league}) AS total_sum
                            FROM DS_2024_{type.upper()}
                            GROUP BY user_id, user_name
                        ) AS sums
                    ) AS ranks
                ) AS ua
            ) AS ranked_users
            WHERE row_num <= 10;
        """
    elif type in ["masters"]:
        query = f"""
            SELECT user_name, total_sum as total_{league}_{type}, dense_rank as rank_{league}_{type}
            FROM (
                SELECT user_id, user_name, total_sum, dense_rank,
                    ROW_NUMBER() OVER (ORDER BY dense_rank ASC) AS row_num
                FROM (
                    SELECT user_id, user_name, total_sum, dense_rank
                    FROM (
                        SELECT user_id, user_name, total_sum,
                            ROW_NUMBER() OVER (ORDER BY total_sum DESC) AS dense_rank
                        FROM (
                            SELECT user_id, user_name, SUM({league}_groups + {league}_playoffs) AS total_sum
                            FROM DS_2024_MASTERS
                            GROUP BY user_id, user_name
                        ) AS sums
                    ) AS ranks
                ) AS ua
            ) AS ranked_users
            WHERE row_num <= 10;
        """
    elif type in ["champions"]:
        query = f"""
            SELECT user_name, total_sum as total_{league}_{type}, dense_rank as rank_{league}_{type}
            FROM (
                SELECT user_id, user_name, total_sum, dense_rank,
                    ROW_NUMBER() OVER (ORDER BY dense_rank ASC) AS row_num
                FROM (
                    SELECT user_id, user_name, total_sum, dense_rank
                    FROM (
                        SELECT user_id, user_name, total_sum,
                            ROW_NUMBER() OVER (ORDER BY total_sum DESC) AS dense_rank
                        FROM (
                            SELECT user_id, user_name, SUM({league}_groups + {league}_playoffs) AS total_sum
                            FROM DS_2024_CHAMPIONS
                            GROUP BY user_id, user_name
                        ) AS sums
                    ) AS ranks
                ) AS ua
            ) AS ranked_users
            WHERE row_num <= 10;
        """
    
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    
    # Return the result as a DataFrame
    df = pd.DataFrame(result, columns=['user_name', f'total_{league}', f'rank_{league}'])
    df = df.rename(columns={'user_name' : 'Username', f'total_{league}': 'Points', f'rank_{league}': 'Rank'})
    df = df.head(10)
    return df

# Get points and rank for a specific user for a type
def get_type_leaderboard(conn, type, username=None, user_id=None):
    cursor = conn.cursor()

    if type in ["il1", "il2", "kickoff"]:
        query = f"""
            SELECT 
                user_name,
                total_sum as total_{type},
                rank_{type}
            FROM (
                SELECT 
                    user_id,
                    user_name,
                    total_sum,
                    ROW_NUMBER() OVER (ORDER BY total_sum DESC) AS rank_{type}
                FROM (
                    SELECT 
                        user_id,
                        user_name,
                        SUM(emea + china + pacific + americas) AS total_sum,
                        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY emea + china + pacific + americas DESC) AS rank_{type}
                    FROM DS_2024_{type.upper()}
                    GROUP BY user_id, user_name
                ) AS sums
            ) AS ranked_users
            WHERE rank_{type} <= 10;
        """
    elif type in ["masters"]:
        query = f"""
            SELECT 
                user_name,
                total_sum as total_{type},
                rank_{type}
            FROM (
                SELECT 
                    user_id,
                    user_name,
                    total_sum,
                    ROW_NUMBER() OVER (ORDER BY total_sum DESC) AS rank_{type}
                FROM (
                    SELECT 
                        user_id,
                        user_name,
                        SUM(shanghai_groups + shanghai_playoffs + madrid_groups + madrid_playoffs) AS total_sum
                    FROM DS_2024_MASTERS
                    GROUP BY user_id, user_name
                ) AS sums
            ) AS ranked_users
            WHERE rank_{type} <= 10;
        """
    elif type in ["champions"]:
        query = f"""
            SELECT 
                user_name,
                total_sum as total_{type},
                rank_{type}
            FROM (
                SELECT 
                    user_id,
                    user_name,
                    total_sum,
                    ROW_NUMBER() OVER (ORDER BY total_sum DESC) AS rank_{type}
                FROM (
                    SELECT 
                        user_id,
                        user_name,
                        SUM(korea_groups + korea_playoffs) AS total_sum
                    FROM DS_2024_CHAMPIONS
                    GROUP BY user_id, user_name
                ) AS sums
            ) AS ranked_users
            WHERE rank_{type} <= 10;
        """
    
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()


    # Return the result as a DataFrame
    df = pd.DataFrame(result, columns=['user_name', f'total_{type}', f'rank_{type}'])
    df = df.rename(columns={'user_name' : 'Username', f'total_{type}': 'Points', f'rank_{type}': 'Rank'})
    df = df.head(10)
    return df

# Get points and rank for a specific user for all year long
def get_all_leaderboard(conn, username=None,user_id=None):
    cursor = conn.cursor()

    query = f"""
        SELECT 
            user_name,
            overall_total,
            dense_rank
        FROM (
            SELECT 
                user_name,
                user_id,
                overall_total,
                ROW_NUMBER() OVER (ORDER BY overall_total DESC) AS dense_rank
            FROM (
                SELECT 
                    user_name, 
                    user_id, 
                    SUM(total_sum) AS overall_total
                FROM (
                    SELECT 
                        user_name, 
                        user_id, 
                        americas + emea + china + pacific AS total_sum
                    FROM DS_2024_KICKOFF

                    UNION ALL

                    SELECT 
                        user_name, 
                        user_id, 
                        americas + emea + china + pacific AS total_sum
                    FROM DS_2024_IL1

                    UNION ALL

                    SELECT 
                        user_name, 
                        user_id, 
                        americas + emea + china + pacific AS total_sum
                    FROM DS_2024_IL2

                    UNION ALL

                    SELECT 
                        user_name, 
                        user_id, 
                        shanghai_groups + shanghai_playoffs + madrid_groups + madrid_playoffs AS total_sum
                    FROM DS_2024_MASTERS

                    UNION ALL

                    SELECT 
                        user_name, 
                        user_id, 
                        korea_groups + korea_playoffs AS total_sum
                    FROM DS_2024_CHAMPIONS
                ) AS all_tables
                GROUP BY user_name, user_id
            ) AS sums
        ) AS ranked_users
        WHERE dense_rank <= 10;
    """

    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()

    # Return the result as a DataFrame
    df = pd.DataFrame(result, columns=['user_name', f'overall_total', f'dense_rank'])
    df = df.rename(columns={'user_name' : 'Username', f'overall_total': 'Points', f'dense_rank': 'Rank'})
    df = df.head(10)
    return df

