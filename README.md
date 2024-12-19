# setup component

docker compose up 

# setup risingwave

sudo apt update && sudo apt install postgresql-client

## 创建 kafka topic 关联

```
-- 创建 Kafka 连接
CREATE SOURCE IF NOT EXISTS event_source (
   event_type VARCHAR,
   url VARCHAR,
   scroll_value INT,
   time BIGINT,
   user_id VARCHAR
)
WITH (
   connector='kafka',
   topic='event-topic',
   properties.bootstrap.server='kafka:9092',
   scan.startup.mode='earliest'
) FORMAT PLAIN ENCODE JSON;
```

## 创建物化视图
```
CREATE MATERIALIZED VIEW user_url_metrics AS
SELECT
    user_id,
    url,
    COUNT(*) FILTER (WHERE event_type = 'visit') AS visit_count,      -- 访问次数
    MAX(scroll_value) FILTER (WHERE event_type = 'scroll') AS max_scroll_value, -- 最大滚动值
    COUNT(*) FILTER (WHERE event_type = 'stay') AS stay_count         -- 停留事件数量
FROM event_source
GROUP BY user_id, url;
```

SELECT * from user_url_metrics LIMIT 10;