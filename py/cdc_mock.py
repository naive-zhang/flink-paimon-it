import random
import string
import datetime
import json
import pymysql
from matplotlib.backend_tools import cursors

# 数据库连接配置
db_config = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'database': 'inventory'
}

# 连接数据库
conn = pymysql.connect(**db_config)
cursor = conn.cursor()

cursor.execute("""CREATE TABLE IF NOT EXISTS user_profile (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    age INT,
    gender ENUM('male', 'female', 'other') DEFAULT 'other',
    birthday DATE,
    balance DECIMAL(10, 2) DEFAULT 0.00,
    address TEXT,
    details JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    last_login DATETIME  -- 新增 DATETIME 字段
);""")
conn.commit()

# 辅助函数：生成随机字符串
def random_string(length=10):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

# 辅助函数：生成随机日期
def random_date(start, end):
    return start + datetime.timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

# 辅助函数：生成随机DATETIME
def random_datetime():
    return datetime.datetime.now() - datetime.timedelta(
        days=random.randint(0, 365),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )

# 生成存量数据
def generate_initial_data(num_records=100):
    for _ in range(num_records):
        name = random_string(8)
        age = random.randint(18, 65) if random.random() > 0.1 else None  # 10% 几率 NULL
        gender = random.choice(['male', 'female', 'other'])
        birthday = random_date(datetime.date(1950, 1, 1), datetime.date(2000, 12, 31)) if random.random() > 0.2 else None
        balance = round(random.uniform(0, 10000), 2)
        address = random_string(50) if random.random() > 0.5 else None  # 50% 几率 NULL
        details = json.dumps({'hobbies': random.sample(['reading', 'sports', 'music', 'travel'], 2)}) if random.random() > 0.4 else None
        last_login = random_datetime() if random.random() > 0.2 else None  # 20% 几率 NULL

        sql = """
        INSERT INTO user_profile (name, age, gender, birthday, balance, address, details, last_login)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (name, age, gender, birthday, balance, address, details, last_login))
    conn.commit()
    print(f"Inserted {num_records} initial records.")

# 模拟增量数据操作
def simulate_incremental_operations(num_operations=50):
    for _ in range(num_operations):
        operation = random.choice(['insert', 'update', 'delete'])
        if operation == 'insert':
            # 插入新记录
            name = random_string(8)
            age = random.randint(18, 65)
            gender = random.choice(['male', 'female', 'other'])
            birthday = random_date(datetime.date(1950, 1, 1), datetime.date(2000, 12, 31))
            balance = round(random.uniform(0, 10000), 2)
            address = random_string(50)
            details = json.dumps({'hobbies': random.sample(['reading', 'sports', 'music', 'travel'], 2)})
            last_login = random_datetime()

            sql = """
            INSERT INTO user_profile (name, age, gender, birthday, balance, address, details, last_login)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (name, age, gender, birthday, balance, address, details, last_login))
            print("Inserted a new record.")

        elif operation == 'update':
            # 更新现有记录
            cursor.execute("SELECT id FROM user_profile ORDER BY RAND() LIMIT 1")
            result = cursor.fetchone()
            if result:
                user_id = result[0]
                new_age = random.randint(18, 65)
                new_balance = round(random.uniform(0, 10000), 2)
                new_last_login = random_datetime()
                sql = "UPDATE user_profile SET age = %s, balance = %s, last_login = %s WHERE id = %s"
                cursor.execute(sql, (new_age, new_balance, new_last_login, user_id))
                print(f"Updated record with id {user_id}.")

        elif operation == 'delete':
            # 删除现有记录
            cursor.execute("SELECT id FROM user_profile ORDER BY RAND() LIMIT 1")
            result = cursor.fetchone()
            if result:
                user_id = result[0]
                sql = "DELETE FROM user_profile WHERE id = %s"
                cursor.execute(sql, (user_id,))
                print(f"Deleted record with id {user_id}.")

    conn.commit()

# 主函数
if __name__ == "__main__":
    # 生成存量数据
    # generate_initial_data(100)

    # 模拟增量操作
    simulate_incremental_operations(50)

    # 关闭连接
    cursor.close()
    conn.close()