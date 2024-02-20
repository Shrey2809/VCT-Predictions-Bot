import pandas as pd

# Get points and rank for specific user for a region
def get_league_sum(conn, league, username=None, user_id=None):
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    if league in ["china", "pacific", "americas", "emea"]:
        query = f"""
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, kickoff_{league} + il1_{league} + il2_{league} AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE user_name = '{username}' OR user_id = {user_id};
        """
    elif league in ["madrid", "shanghai"]:
        query = f"""
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, masters_{league}_playoffs + masters_{league}_groups AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE user_name = '{username}' OR user_id = {user_id};
        """
    elif league in ["korea"]:
        query = f"""
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, champions_{league}_playoffs + champions_{league}_groups AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE user_name = '{username}' OR user_id = {user_id};
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
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, {type}_{league} AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE user_name = '{username}' OR user_id = {user_id};
        """
    elif type in ["masters"]:
        query = f"""
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, {type}_{league}_playoffs + {type}_{league}_groups AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE user_name = '{username}' OR user_id = {user_id};
        """
    elif type in ["champions"]:
        query = f"""
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, {type}_{league}_playoffs + {type}_{league}_groups AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE user_name = '{username}' OR user_id = {user_id};
        """
    
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    
    df = pd.DataFrame(result, columns=['user_name', f'total_{league}_{type}', f'rank_{league}_{type}'])
    df = df.rename(columns={f'total_{league}_{type}': 'Points', f'rank_{league}_{type}': 'Rank'})
    return df

# Get points and rank for a specific user for a type
def get_type_sum(conn, type, username=None, user_id=None):
    cursor = conn.cursor()

    if type in ["il1", "il2", "kickoff"]:
        query = f"""
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, {type}_americas + {type}_emea + {type}_pacific + {type}_china AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE user_name = '{username}' OR user_id = {user_id};
        """
    elif type in ["masters"]:
        query = f"""
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, {type}_shanghai_groups + {type}_shanghai_playoffs + {type}_madrid_groups + {type}_madrid_playoffs AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE user_name = '{username}' OR user_id = {user_id};
        """
    elif type in ["champions"]:
        query = f"""
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, {type}_korea_groups + {type}_korea_playoffs AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE user_name = '{username}' OR user_id = {user_id};
        """
    
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    df = pd.DataFrame(result, columns=['user_name', f'total_{type}', f'rank_{type}'])
    df = df.rename(columns={f'total_{type}': 'Points', f'rank_{type}': 'Rank'})
    return df

# Get points and rank for a specific user for all year long
def get_all_sum(conn, username=None,user_id=None):
    cursor = conn.cursor()

    query = f"""
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, 
                                masters_shanghai_playoffs + masters_shanghai_groups + 
                                masters_madrid_playoffs + masters_madrid_groups + 
                                champions_korea_groups + champions_korea_playoffs + 
                                kickoff_americas + kickoff_emea + kickoff_china + kickoff_pacific +
                                il1_americas + il1_emea + il1_china + il1_pacific +
                                il2_americas + il2_emea + il2_china + il2_pacific AS val                                
                                FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE user_name = '{username}' OR user_id = {user_id};
    """

    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()

    df = pd.DataFrame(result, columns=['user_name', 'overall_total', 'dense_rank'])
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
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, kickoff_{league} + il1_{league} + il2_{league} AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE overall_rank <= 10;
        """
    elif league in ["madrid", "shanghai"]:
        query = f"""
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, masters_{league}_playoffs + masters_{league}_groups AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE overall_rank <= 10;
        """
    elif league in ["korea"]:
        query = f"""
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, champions_{league}_playoffs + champions_{league}_groups AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE overall_rank <= 10;
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
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, {type}_{league} AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE overall_rank <= 10;
        """
    elif type in ["masters"]:
        query = f"""
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, {type}_{league}_playoffs + {type}_{league}_groups AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE overall_rank <= 10;
        """
    elif type in ["champions"]:
        query = f"""
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, {type}_{league}_playoffs + {type}_{league}_groups AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE overall_rank <= 10;
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
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, {type}_americas + {type}_emea + {type}_pacific + {type}_china AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE overall_rank <= 10;
        """
    elif type in ["masters"]:
        query = f"""
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, {type}_shanghai_groups + {type}_shanghai_playoffs + {type}_madrid_groups + {type}_madrid_playoffs AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE overall_rank <= 10;
        """
    elif type in ["champions"]:
        query = f"""
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, {type}_korea_groups + {type}_korea_playoffs AS val FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE overall_rank <= 10;
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
            SELECT user_name, total_league, rank_league
            FROM (
                SELECT user_name, user_id, total_league, rank_league,
                    ROW_NUMBER() OVER (ORDER BY total_league DESC) AS overall_rank
                FROM (
                    SELECT user_name, user_id, COALESCE(SUM(val), 0) AS total_league,
                        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(val), 0) DESC) AS rank_league
                    FROM (
                        SELECT user_name, user_id, 
                                masters_shanghai_playoffs + masters_shanghai_groups + 
                                masters_madrid_playoffs + masters_madrid_groups + 
                                champions_korea_groups + champions_korea_playoffs + 
                                kickoff_americas + kickoff_emea + kickoff_china + kickoff_pacific +
                                il1_americas + il1_emea + il1_china + il1_pacific +
                                il2_americas + il2_emea + il2_china + il2_pacific AS val                                
                                FROM DS_VCT_2024
                    ) AS ua
                    GROUP BY user_name, user_id
                ) AS ranked_users
            ) AS final_rank
            WHERE overall_rank <= 10;
    """

    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()

    # Return the result as a DataFrame
    df = pd.DataFrame(result, columns=['user_name', f'overall_total', f'dense_rank'])
    df = df.rename(columns={'user_name' : 'Username', f'overall_total': 'Points', f'dense_rank': 'Rank'})
    df = df.head(10)
    return df

