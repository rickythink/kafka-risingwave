import sys
import time
import json
import random
import string
from threading import Thread, Lock
import psycopg2

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaProducer

class ThreadSafeCounter:
    def __init__(self):
        self._counts = {}
        self._lock = Lock()
    
    def increment(self, user_id, url):
        with self._lock:
            if user_id not in self._counts:
                self._counts[user_id] = {}
            if url not in self._counts[user_id]:
                self._counts[user_id][url] = 0
            self._counts[user_id][url] += 1
            return self._counts[user_id][url]
    
    def get_count(self, user_id, url):
        with self._lock:
            return self._counts.get(user_id, {}).get(url, 0)

producer = KafkaProducer(
    bootstrap_servers="localhost:9093",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

visit_counts = ThreadSafeCounter()

def generate_event(user_id):
    """随机生成一个事件"""
    now = int(time.time() * 1000)
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

def send_event_with_latency_check(user_id, topic, request_per_sec, duration, db_conn):
    delay = 1 / request_per_sec
    end_time = time.time() + duration
    while time.time() < end_time:
        event = generate_event(user_id)
        producer.send(topic, event)
        url = event["url"]

        if event["event_type"] == "visit":
            current_count = visit_counts.increment(user_id, url)
            # print(f"user: {user_id} url: {url} visit now: {current_count}")

        if event["event_type"] == "trigger":
            expected_visit_count = visit_counts.get_count(user_id, url)
            if expected_visit_count > 0:
                check_risingwave(user_id, url, expected_visit_count, db_conn)

        time.sleep(delay)

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

            if result:
                actual_visit_count = result[0]
                if actual_visit_count == expected_visit_count:
                    elapsed_ms = (time.time() - start_time) * 1000  # 转换为毫秒
                    # print(f"✅ Query successful after {elapsed_ms:.1f}ms: user {user_id}, url {url}, visit_count = {actual_visit_count} (expected)")
                    return elapsed_ms
                else:
                    # 延迟转换为毫秒显示
                    delay_ms = delay * 100  # 0.1秒转换为毫秒
                    print(f"⚠️ Query mismatch after {delay_ms}ms: user {user_id}, url {url}, visit_count = {result[0]}, expected = {expected_visit_count}")

        delay += 0.1  # 改为 0.1 秒延迟
        time.sleep(0.1)  # 改为 0.1 秒

    # 最大延迟转换为毫秒显示
    max_delay_ms = max_delay * 1000
    print(f"❌ Query failed after {max_delay_ms}ms: user {user_id}, url {url}")

def generate_user_id():
    """生成格式为 user_[6位随机字符串] 的 user_id"""
    random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=6))
    return f"user_{random_str}"

if __name__ == "__main__":
    db_conn = psycopg2.connect(
        host="localhost",
        port=4566,
        database="dev",
        user="root",
        password=""
    )

    USER = 5000
    REQUEST_PER_SEC = 5
    DURATION = 10
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