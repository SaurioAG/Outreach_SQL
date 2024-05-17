# Outreach_SQL
SQL task: In order to solve the first two tasks, I generated a Python script to handle a set of task in order to create a DB with random data and be able to query to it.
You will find code blocks and result images that solves each of the inquiries.

## Dataset 1

Imagine a Keyboard_Typing Table

It has columns: Key_Stroke_At, Session_id, User_id, Key_Stroke

KEY_STROKE_AT|| SESSION_ID || USER_ID || KEY_STROKE

01-01-2022 12:04:43 || "ABC" || "NICK" || "A"

01-01-2022 22:26:53 || "ABC" || "NICK" || "ENTER"

01-01-2022 22:26:53 || "AD" || "RICHARD" || "B"

Question 1: How would you write a query that gives me the Average # of keystrokes per session yesterday?

In order to address this first question, I had to generate some random data to create the Keyboard_Typing Table and then populate it into a sqlite DB table.

```
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

db_name = "keyboard.db"
keyboard_table = "keyboard_typing"

data = generate_keyboard_typing_data()
connection = sqlite_conn(db_name)
cursor = sqlite_cursor(connection)
populate_to_db(data, connection, keyboard_table)
average = avg_keystrokes_session(cursor, keyboard_table)
print(average)
```

Ther results will look like:

keyboard_typing table:

![keyboard_typing_table](https://github.com/SaurioAG/Outreach_SQL/assets/167505635/88f43dc4-ade5-4afc-a1fb-bd1aa2bcfe89)

Query used:

```
SELECT
	AVG(keystrokes) AS avg_keystrokes_per_session_yesterday
FROM (
	SELECT 
		Session_id,
		count(Session_id) AS keystrokes
	FROM keyboard_typing 
	WHERE Key_Stroke_At LIKE '2024-05-14%'
	GROUP BY Session_id);
```

Dataframe:

![avg_keystrokes_session](https://github.com/SaurioAG/Outreach_SQL/assets/167505635/9348ace5-e3f5-4fbf-9d53-fcdbbafa8ed0)

Query result:

![avg_keystrokes_session_query](https://github.com/SaurioAG/Outreach_SQL/assets/167505635/1401f9e2-8484-4958-b9bf-86e31b410a8e)


=================================================================================================


Question 2: How would you write a query whose output looks like the following:

Date || a_key_strokes || b_key_strokes || all_other_keys

01/01/2021 || 17 || 590 || 99999

01/02/2021 || 352 || 3455 || 18


```
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

keystrokes = keystrokes_per_date(cursor, keyboard_table)
print(keystrokes)
```

Ther results will look like:

Dataframe:

![keystrokes_per_date](https://github.com/SaurioAG/Outreach_SQL/assets/167505635/919196bf-d7df-45fe-a90b-b74b22ea4d4e)


Query used:

```
SELECT
    DATE(Key_Stroke_At) AS Date,
    SUM(CASE WHEN Key_Stroke = 'a' THEN 1 ELSE 0 END) AS a_key_strokes,
    SUM(CASE WHEN Key_Stroke = 'b' THEN 1 ELSE 0 END) AS b_key_strokes,
    SUM(CASE WHEN Key_Stroke NOT IN ('a', 'b') THEN 1 ELSE 0 END) AS all_other_keys
FROM
    keyboard_typing
GROUP BY
    DATE(Key_Stroke_At);
```

Query result:

![keystrokes_per_date_query](https://github.com/SaurioAG/Outreach_SQL/assets/167505635/a68e97ba-3d32-4bb5-b325-64809f01bcfe)


## Dataset 2

Imagine 1) a key subscription table that pre-aggregates at the user level, key subscription product, ARR (annual recurring revenue), subscription start and end.

USER || START_REV_REQ || END_REV_REQ|| PRODUCT || ARR

Nick || 01/01/2021 || 01/03/2021 || Q || $17

Nick || 01/03/2021 || 01/06/2021 || Q || $17

Nick || 01/03/2021 || 01/06/2021 || Enter || $3


and 2) a date table like below:

DATE || FISCAL_QUARTER

01/01/2021 || 2022/Q3

01/02/2021 || 2022/Q3

01/03/2021 || 2022/Q3


Question 3: How would you write a query that gives for every day, for every user; how much revenue did each user have?

In the same way I had to generate random data that could be queried later:

```
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

subs_table = "key_subscription_table"
fiscal_table = "fiscal_quarter_table"
data = generate_subs_data()

populate_to_db(data[0], connection, subs_table)
populate_to_db(data[1], connection, fiscal_table)

daily_revenue = daily_rev_user(cursor, subs_table, fiscal_table)
print(daily_revenue)
```

Ther results will look like:

key_subscription_table table:

![key_subscription_table](https://github.com/SaurioAG/Outreach_SQL/assets/167505635/244916d2-058d-47ae-9433-c25fcdb4af2d)


fiscal_quarter_table table:

![fiscal_quarter_table](https://github.com/SaurioAG/Outreach_SQL/assets/167505635/94849a5b-c628-4b16-9c7b-0ddc0ddd3b98)

Dataframe:

![daily_revenue](https://github.com/SaurioAG/Outreach_SQL/assets/167505635/83d0e9a0-6def-40d0-bfa0-9f1e61461db3)

Query used:

```
SELECT
    fqt.DATE,
    fqt.FISCAL_QUARTER,
    kst.USER,
    SUM(kst.ARR) AS daily_revenue
FROM
    fiscal_quarter_table fqt
LEFT JOIN
    key_subscription_table kst ON fqt.DATE >= kst.START_REV_REQ
								AND fqt.DATE <=  kst.END_REV_REQ 
GROUP BY
    fqt.DATE,
    fqt.FISCAL_QUARTER,
    kst.USER;
```

Query result:

![daily_revenue_query](https://github.com/SaurioAG/Outreach_SQL/assets/167505635/49cccfe4-9930-4a94-bd70-07699fefe862)


============================================================================

Question 4: Can you do as above, but also per day, per user, per key subscription?

```
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

daily_revenue_product = daily_rev_user_product(cursor, subs_table, fiscal_table)
print(daily_revenue_product)
```

The results look like:

Dataframe:

![daily_revenue_product](https://github.com/SaurioAG/Outreach_SQL/assets/167505635/8781e816-a8df-4b42-84a1-4d77e9b5dfa7)

Query:

```
SELECT
    fqt.DATE,
    fqt.FISCAL_QUARTER,
    kst.USER,
	kst.PRODUCT,
    SUM(kst.ARR) AS daily_revenue
FROM
    fiscal_quarter_table fqt
LEFT JOIN
    key_subscription_table kst ON fqt.DATE >= kst.START_REV_REQ
								AND fqt.DATE <=  kst.END_REV_REQ 
GROUP BY
    fqt.DATE,
    fqt.FISCAL_QUARTER,
    kst.USER,
	kst.PRODUCT;
```

Query result:

![daily_revenue_product_query](https://github.com/SaurioAG/Outreach_SQL/assets/167505635/088a0f37-1d9c-4213-b97f-76ee2f826e0f)


===============================================================================


Question 5: Can you find the first subscription, for every key subscription, per user?

```
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

first_subs = sub_user_product(cursor, subs_table)
print(first_subs)
connection.close()
```

The results look like:

Dataframe:

![first_subscription](https://github.com/SaurioAG/Outreach_SQL/assets/167505635/8eea308d-e6d4-4e60-b480-d7280e0ce476)

Query:

```
SELECT
	USER,
	PRODUCT,
	MIN(START_REV_REQ) AS first_subscription_start
FROM
	key_subscription_table
GROUP BY
	USER,
	PRODUCT;
```

Query retults:

![first_subscription_query](https://github.com/SaurioAG/Outreach_SQL/assets/167505635/ffe3521a-eb80-4a31-9f75-36f64edf071c)


## In Depth

Q1. Write SQL query to only load new rows from staging_users into prod_users

Suppose staging_users table is set up as

CREATE TABLE staging_users (

id int NOT NULL,

first_name varchar,

role varchar,

locked boolean,

locked_at timestamp,

batch_start_time timestamp,

batch_id int

);


And prod_user is set up as

CREATE TABLE prod_users (

user_id int NOT NULL,

first_name varchar,

user_role varchar,

user_is_locked boolean,

user_locked_at timestamp,

batch_start_time timestamp,

batch_id int


Answer Query:
```
INSERT INTO prod_users (user_id, first_name, user_role, user_is_locked, user_locked_at, batch_start_time, batch_id)
SELECT 
    s.id AS user_id, 
    s.first_name, 
    s.role AS user_role, 
    s.locked AS user_is_locked, 
    s.locked_at AS user_locked_at, 
    s.batch_start_time, 
    s.batch_id
FROM 
    staging_users s
LEFT JOIN 
    prod_users p ON s.id = p.user_id
WHERE 
    p.user_id IS NULL;
```

=============================================================================


Q2. Write SQL query to only load the most recent record of each user from event_stream_users

Suppose event_stream_users captures all dml actions of the user records and returns a row with the updated values. Only return the most recent record

Table is setup as follows:

CREATE TABLE event_stream_users (

id int NOT NULL,

first_name varchar,

role varchar,

locked boolean,

locked_at timestamp,

dml_at timestamp,

dml_type varchar,

batch_start_time timestamp,

batch_id int

);


Answer Query:
```
WITH MostRecentEvent AS (
    SELECT
        id,
        first_name,
        role,
        locked,
        locked_at,
        dml_at,
        dml_type,
        batch_start_time,
        batch_id,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY dml_at DESC) AS events
    FROM
        event_stream_users)
SELECT
    id,
    first_name,
    role,
    locked,
    locked_at,
    dml_at,
    dml_type,
    batch_start_time,
    batch_id
FROM
    MostRecentEvent
WHERE
    events = 1;
```

=============================================================================


Q3. Write SQL query to design an SCD for user roles

From the same event_stream_users table write SQL to produce an SCD for user roles showing changes in user roles

event_stream_users is set up as

CREATE TABLE event_stream_users (

id int NOT NULL,

first_name varchar,

role varchar,

locked boolean,

locked_at timestamp,

dml_at timestamp,

dml_type varchar,

batch_start_time timestamp,

batch_id int

);

Expected return

user_id,

user_role,

effective_at,

expiration_at


Answer Query:
```
WITH RankedRoles AS (
    SELECT
        id AS user_id,
        role AS user_role,
        dml_at AS effective_at,
        LEAD(dml_at) OVER (PARTITION BY id ORDER BY dml_at) AS expiration_at
    FROM
        event_stream_users
    WHERE
        dml_type = 'UPDATE' OR dml_type = 'INSERT'
)
SELECT
    user_id,
    user_role,
    effective_at,
    COALESCE(expiration_at, '2025-12-31') AS expiration_at
FROM
    RankedRoles
ORDER BY
    user_id,
    effective_at;
```

## Installation

If you want to execute the python script that address the first two sections (Dataset1, Dataset2)
You are going to need:

DB:

SQLite Version 3.12.2

Python:

Python 3.10

Libraries:

```
pip install sqlite3 pandas
```
