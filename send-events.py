import sys
import time
import json
import random
import string
from threading import Thread
import psycopg2  # RisingWave uses PostgreSQL interface

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9093",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# 定义事件生成逻辑
def generate_event(user_id):
    """随机生成一个事件"""
    now = int(time.time() * 1000)  # 当前时间戳（毫秒）
    event_type = random.choice(["visit", "scroll", "stay", "trigger"])
    url = random.choice(URLS)

    if event_type == "visit":
        return {"event_type": "visit", "url": url, "time": now, "user_id": user_id}
    elif event_type == "scroll":
        scroll_value = random.randint(100, 1000)
        return {"event_type": "scroll", "url": url, "scroll_value": scroll_value, "time": now, "user_id": user_id}
    elif event_type == "stay":
        return {"event_type": "stay", "url": url, "time": now, "user_id": user_id}
    elif event_type == "trigger":
        return {"event_type": "trigger", "url": url, "time": now, "user_id": user_id}

# 发送事件逻辑
def send_event_with_latency_check(user_id, topic, request_per_sec, duration, db_conn):
    delay = 1 / request_per_sec
    end_time = time.time() + duration
    visit_counts = {}  # 记录每个 URL 的访问次数
    while time.time() < end_time:
        event = generate_event(user_id)
        producer.send(topic, event)
        print(f"Sent event: {event}")

        # 更新访问计数
        if event["event_type"] == "visit":
            visit_counts[event["url"]] = visit_counts.get(event["url"], 0) + 1

        # 如果是 trigger 事件，则进行数据库查询
        if event["event_type"] == "trigger":
            url = event["url"]
            expected_visit_count = visit_counts.get(url, 0)
            check_risingwave(user_id, url, expected_visit_count, db_conn)

        time.sleep(delay)# 发送事件逻辑
def send_event_with_latency_check(user_id, topic, request_per_sec, duration, db_conn):
    delay = 1 / request_per_sec
    end_time = time.time() + duration
    visit_counts = {}  # 记录每个 URL 的访问次数
    while time.time() < end_time:
        event = generate_event(user_id)
        producer.send(topic, event)
        # print(f"Sent event: {event}")
        url = event["url"]

        # 更新访问计数
        if event["event_type"] == "visit":
            visit_counts[user_id] = visit_counts.get(user_id, {}) 
            visit_counts[user_id][url] = visit_counts.get(url, 0) + 1
            print(f"user: {user_id} user: {url} visit now: {visit_counts[user_id][url]}")

        # 如果是 trigger 事件，且现在有 visit 次数, 则进行数据库查询
        if event["event_type"] == "trigger" and visit_counts.get(user_id, {}).get(url, 0):
            expected_visit_count = visit_counts.get(user_id, {}).get(url, 0)
            check_risingwave(user_id, url, expected_visit_count, db_conn)

        time.sleep(delay)

# 检查数据库逻辑，增加延迟查询和延迟时间测量
def check_risingwave(user_id, url, expected_visit_count, db_conn, max_delay=5):
    """延迟查询 RisingWave 中的视图数据并验证 visit_count"""
    start_time = time.time()
    delay = 0

    while delay <= max_delay:
        with db_conn.cursor() as cursor:
            query = f"""
            SELECT visit_count
            FROM user_url_metrics
            WHERE user_id = '{user_id}' AND url = '{url}';
            """
            cursor.execute(query)
            result = cursor.fetchone()

            # 如果查到数据并且符合预期，则记录延迟
            if result:
                actual_visit_count = result[0]
                if actual_visit_count == expected_visit_count:
                    elapsed_time = time.time() - start_time
                    print(f"✅ Query successful after {elapsed_time:.1f}s: user {user_id}, url {url}, visit_count = {actual_visit_count} (expected)")
                    return elapsed_time
                else:
                    print(f"❌ Query mismatch after {delay}s: user {user_id}, url {url}, visit_count = {result[0]}, expected = {expected_visit_count}")

        delay += 1
        time.sleep(1)  # 每次延迟 1 秒进行查询


    print(f"⚠️ Query failed after {max_delay}s: user {user_id}, url {url}")

def generate_user_id():
    """生成格式为 user_[6位随机字符串] 的 user_id"""
    random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=6))
    return f"user_{random_str}"

if __name__ == "__main__":
    # 数据库连接设置
    db_conn = psycopg2.connect(
        host="localhost",
        port=4566,
        database="dev",
        user="root",
        password=""
    )

    USER = 10  # 模拟用户数
    REQUEST_PER_SEC = 5  # 每秒请求数
    DURATION = 10  # 持续时间（秒）
    KAFKA_TOPIC = "event-topic"
    URLS = [f"https://example.com/page{i}" for i in range(1, 11)]

    threads = []
    for i in range(USER):
        user_id = generate_user_id()
        thread = Thread(target=send_event_with_latency_check, args=(user_id, KAFKA_TOPIC, REQUEST_PER_SEC, DURATION, db_conn))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print("Simulation completed.")