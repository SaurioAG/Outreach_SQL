import random
import datetime
import pandas as pd
import sqlite3
import os

def give_current_datetime():
    """
    Returns actual timestamp
    """
    return datetime.datetime.now()

def generate_keyboard_typing_data():
    """
    Generates random data to populate keyboad_typing_table on sqlite database
    """
    characters = "abcdefghijklmnñopqrstuvwxyz0123456789,.-{+}"
    dates = [give_current_datetime(), give_current_datetime() - datetime.timedelta(days = 1)]
    user_dict = {
        "s1": "user1",
        "s2": "user2",
        "s3": "user3",
        "s4": "user4"}
    list_users = []
    data = [(random.choice(dates),
             random.choice(characters),
             random.choice(list(user_dict.keys()))) for i in range(0,100)]
    dataframe = pd.DataFrame(data, columns = ["Key_Stroke_At", "Key_Stroke", "Session_id"])
    for id in dataframe["Session_id"]:
        list_users.append(user_dict[id])
    dataframe["User_id"] = list_users
    return dataframe

def sqlite_conn(db_name):
    """
    Creates a connection to a sqlite database
    """
    if os.path.exists(db_name):
        os.remove(db_name)
    connection = sqlite3.connect(db_name)
    return connection

def sqlite_cursor(connection):
    """
    Creates a cursor to handle DML to the sqlite database
    """
    cursor = connection.cursor()
    return cursor

def populate_to_db(dataframe, connection, table_name):
    """
    Populates tables on the sqlite database, using the previous randomly generated data
    """
    try:
        dataframe.to_sql(table_name, connection, index =  False)
    except ValueError:
        print(f'Table {table_name} already exist')

def avg_keystrokes_session(cursor, table_name):
    """
    Gives the Average number of keystrokes per session yesterday
    You can set the desired "yesterday" date on the query at line

    WHERE Key_Stroke_At LIKE '2024-05-15%'

    Depending on the dates being used on the db table
    """
    # buscar manera de hacer dinamico el valor de la fecha en "WHERE Key_Stroke_At LIKE '2024-05-14%'"
    query = f'''
            SELECT\
                AVG(keystrokes) AS avg_keystrokes_per_session_yesterday\
            FROM (\
                SELECT \
                    Session_id,\
                    count(Session_id) AS keystrokes\
                FROM {table_name}\
                WHERE Key_Stroke_At LIKE '2024-05-15%'\
                GROUP BY Session_id)
            '''
    cursor.execute(query)
    result = cursor.fetchall()
    dataframe = pd.DataFrame(result, columns = ["avg_keystrokes_per_session_yesterday"])
    return dataframe

def keystrokes_per_date(cursor, table_name):
    """
    Generates an ouput like the following table

    Date || a_key_strokes || b_key_strokes || all_other_keys
    01/01/2021 || 17 || 590 || 99999
    01/02/2021 || 352 || 3455 || 18
    """
    query = f'''
            SELECT\
                DATE(Key_Stroke_At) AS Date,\
                SUM(CASE WHEN Key_Stroke = 'a' THEN 1 ELSE 0 END) AS a_key_strokes,\
                SUM(CASE WHEN Key_Stroke = 'b' THEN 1 ELSE 0 END) AS b_key_strokes,\
                SUM(CASE WHEN Key_Stroke NOT IN ('a', 'b') THEN 1 ELSE 0 END) AS all_other_keys\
            FROM\
                {table_name}\
            GROUP BY\
                DATE(Key_Stroke_At)
            '''
    cursor.execute(query)
    result = cursor.fetchall()
    dataframe = pd.DataFrame(result, columns = ["Date", "a_key_strokes", "b_key_strokes", "all_other_keys"])
    return dataframe

def generate_subs_data():
    """
    Generates random data to populate key_subscription_table on sqlite database
    """
    users = ["user1", "user2", "user3"]
    dates_dict = {
        "01/01/2024": "2025/Q1",
        "01/04/2024": "2025/Q2",
        "01/07/2024": "2025/Q3",
        "01/10/2024": "2025/Q4"}
    date_format = "%d/%m/%Y"
    product = ["item1", "item2", "item3"]
    price = [cost for cost in range(20, 121)]
    data = [(random.choice(users),
             random.choice(product),
             random.choice(price),
             datetime.datetime.strptime(random.choice(list(dates_dict.keys())), date_format).date()) for i in range(0,100)]
    subs_table_df = pd.DataFrame(data, columns = ["USER", "PRODUCT", "ARR", "START_REV_REQ"])
    end_dates = []
    for date in subs_table_df["START_REV_REQ"]:
        end_dates.append(date + datetime.timedelta(days = 90))
    subs_table_df["END_REV_REQ"] = [datetime.datetime.strftime(date, date_format) for date in end_dates]
    subs_table_df["START_REV_REQ"] = [datetime.datetime.strftime(date, date_format) for date in subs_table_df["START_REV_REQ"]]
    subs_table_df = subs_table_df.reindex(columns = ["USER", "START_REV_REQ", "END_REV_REQ", "PRODUCT", "ARR"])
    date_q_df = pd.DataFrame(dates_dict.items(), columns = ["DATE", "FISCAL_QUARTER"])
    return subs_table_df, date_q_df

def daily_rev_user(cursor, subs_table, fiscal_table):
    """
    Gives for every day, for every user; how much revenue did each user
    """
    query = f'''
            SELECT\
                fqt.DATE,\
                fqt.FISCAL_QUARTER,\
                kst.USER,\
                SUM(kst.ARR) AS daily_revenue\
            FROM\
                {fiscal_table} fqt\
            LEFT JOIN\
                {subs_table} kst ON fqt.DATE >= kst.START_REV_REQ\
                                AND fqt.DATE <=  kst.END_REV_REQ \
            GROUP BY\
                fqt.DATE,\
                fqt.FISCAL_QUARTER,\
                kst.USER	
            '''
    cursor.execute(query)
    result = cursor.fetchall()
    dataframe = pd.DataFrame(result, columns = ["DATE", "FISCAL_QUARTER", "USER", "daily_revenue"])
    return dataframe

def daily_rev_user_product(cursor, subs_table, fiscal_table):
    """
    Gives for every day, for every user, per key subscription(PRODUCT); how much revenue did each user
    """
    query = f'''
            SELECT\
                fqt.DATE,\
                fqt.FISCAL_QUARTER,\
                kst.USER,\
                kst.PRODUCT,\
                SUM(kst.ARR) AS daily_revenue\
            FROM\
                {fiscal_table} fqt\
            LEFT JOIN\
                {subs_table} kst ON fqt.DATE >= kst.START_REV_REQ\
                                AND fqt.DATE <=  kst.END_REV_REQ \
            GROUP BY\
                fqt.DATE,\
                fqt.FISCAL_QUARTER,\
                kst.USER,\
                kst.PRODUCT	
            '''
    cursor.execute(query)
    result = cursor.fetchall()
    dataframe = pd.DataFrame(result, columns = ["DATE", "FISCAL_QUARTER", "USER", "PRODUCT", "daily_revenue"])
    return dataframe

def sub_user_product(cursor, subs_table):
    """
    Find the first subscription, for every key subscription, per user
    """
    query = f'''
            SELECT\
                USER,\
                PRODUCT,\
                MIN(START_REV_REQ) AS first_subscription_start\
            FROM\
                {subs_table}\
            GROUP BY\
                USER,\
                PRODUCT
            '''
    cursor.execute(query)
    result = cursor.fetchall()
    dataframe = pd.DataFrame(result, columns = ["USER", "PRODUCT", "first_subscription_start"])
    return dataframe

db_name = "keyboard.db"
keyboard_table = "keyboard_typing"

data = generate_keyboard_typing_data()
connection = sqlite_conn(db_name)
cursor = sqlite_cursor(connection)
populate_to_db(data, connection, keyboard_table)
average = avg_keystrokes_session(cursor, keyboard_table)
print(average)
keystrokes = keystrokes_per_date(cursor, keyboard_table)
print(keystrokes)

subs_table = "key_subscription_table"
fiscal_table = "fiscal_quarter_table"
data = generate_subs_data()

populate_to_db(data[0], connection, subs_table)
populate_to_db(data[1], connection, fiscal_table)

daily_revenue = daily_rev_user(cursor, subs_table, fiscal_table)
daily_revenue_product = daily_rev_user_product(cursor, subs_table, fiscal_table)
first_subs = sub_user_product(cursor, subs_table)

connection.close()

print(daily_revenue)
print(daily_revenue_product)
print(first_subs)




