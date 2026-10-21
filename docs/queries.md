# Saved queries

The pipeline writes every event as JSON into the `value` column of `default.events_raw`. It uses `SELECT *`, so there is no fixed column schema. Read fields with Cloudflare's JSON functions.

Delivery is best-effort. A sink failure can lose an event, and a retry can duplicate one. Count `DISTINCT messageId`, not rows. Sinks flush on their own cadence, so recent writes may not show yet.

Replace `app.example.com` with the `host` your Worker records. Set `host` in `withEventCapture` for a fixed service label. Several Workers can share one stream, so group by host before you count traffic.

## Traffic by path and agent

```sql
SELECT json_get_str(value, 'server', 'host') AS service,
       json_get_str(value, 'body', 'method') AS method,
       json_get_str(value, 'body', 'path') AS path,
       json_get_str(value, 'server', 'userAgent') AS user_agent,
       count(DISTINCT json_get_str(value, 'messageId')) AS requests
FROM default.events_raw
WHERE json_get_str(value, 'server', 'host') = 'app.example.com'
  AND json_get_str(value, 'body', 'type') = 'request'
GROUP BY service, method, path, user_agent
ORDER BY requests DESC;
```

`userAgent` is the raw header the client sent. Bucket it in your own body schema if you need fewer groups.

## 5xx by path and release

`releaseVersion` is not a base field. The example assumes you add it to your own body schema. Replace the two ISO bounds with your observation window.

```sql
WITH requests AS (
  SELECT json_get_str(value, 'messageId') AS id,
         json_get_str(value, 'body', 'path') AS path,
         json_get_str(value, 'body', 'releaseVersion') AS release,
         json_get_int(value, 'body', 'status') AS status
  FROM default.events_raw
  WHERE json_get_str(value, 'server', 'host') = 'app.example.com'
    AND json_get_str(value, 'server', 'receivedAt') >= '2026-01-01T00:00:00.000Z'
    AND json_get_str(value, 'server', 'receivedAt') < '2026-01-01T01:00:00.000Z'
), deduplicated AS (
  SELECT DISTINCT id, path, release, status FROM requests
)
SELECT path, release, count(*) AS requests,
       sum(CASE WHEN status >= 500 THEN 1 ELSE 0 END) AS errors,
       1.0 * sum(CASE WHEN status >= 500 THEN 1 ELSE 0 END) / count(*) AS error_rate
FROM deduplicated
GROUP BY path, release;
```

An absent release, a path with no events, or a pending flush means "unknown". It does not mean "clean".

## Discovery traffic

```sql
SELECT json_get_str(value, 'body', 'path') AS path,
       json_get_str(value, 'body', 'accept') AS accept,
       count(DISTINCT json_get_str(value, 'messageId')) AS requests
FROM default.events_raw
WHERE json_get_str(value, 'server', 'host') = 'app.example.com'
  AND json_get_str(value, 'body', 'path') IN ('/', '/llms.txt', '/robots.txt')
GROUP BY path, accept;
```

## Latency percentiles

`durationMs` runs from the start of the handler until its response exists. It excludes streaming body delivery, background capture and sink flush. Restrict to a `receivedAt` window and one service for operational use.

```sql
WITH requests AS (
  SELECT DISTINCT json_get_str(value, 'messageId') AS id,
         json_get_str(value, 'server', 'host') AS service,
         json_get_str(value, 'body', 'path') AS path,
         json_get_float(value, 'body', 'durationMs') AS duration
  FROM default.events_raw
  WHERE json_get_str(value, 'body', 'type') = 'request'
)
SELECT service, path, count(*) AS requests,
       approx_percentile_cont(duration, 0.50) AS p50_ms,
       approx_percentile_cont(duration, 0.95) AS p95_ms,
       approx_percentile_cont(duration, 0.99) AS p99_ms
FROM requests
GROUP BY service, path;
```

These queries use Cloudflare's documented JSON and aggregate functions. They have not been run against a live catalog from this repository.
