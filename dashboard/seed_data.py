"""
Stable seed for Grafana dashboards (time-safe version)
"""

import random
from datetime import datetime, timedelta, timezone
import psycopg2

PG = dict(
    host="localhost",
    port=5433,
    dbname="securitydb",
    user="secu",
    password="secret"
)

NORMAL_IPS = [f"192.168.0.{i}" for i in range(1, 51)]
BLACKLIST = ["10.66.66.6", "45.137.21.9", "185.220.101.1"]
ACTIONS = ["login_ok", "login_fail", "connect", "port_scan"]


def main():
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()

    cur.execute("TRUNCATE alerts, event_counts RESTART IDENTITY;")

    # ✅ ALWAYS use UTC to match Grafana
    now = datetime.now(timezone.utc)

    # =========================================================
    # event_counts: last 7 days (NOT 60 minutes anymore)
    # =========================================================
    for h in range(7 * 24, 0, -1):  # 7 days hourly buckets
        wstart = now - timedelta(hours=h)
        wend = wstart + timedelta(hours=1)

        for action in ACTIONS:
            base = {
                "login_ok": 600,
                "login_fail": 120,
                "connect": 200,
                "port_scan": 8
            }[action]

            cnt = max(0, int(random.gauss(base, base * 0.25)))

            cur.execute(
                """
                INSERT INTO event_counts (window_start, window_end, action, count)
                VALUES (%s, %s, %s, %s)
                """,
                (wstart, wend, action, cnt),
            )

    # =========================================================
    # alerts: last 30 days (so Grafana "last year" ALWAYS works)
    # =========================================================
    for _ in range(200):  # more data = better dashboard
        days_ago = random.randint(0, 30)
        mins_offset = random.randint(0, 1440)

        ts = now - timedelta(days=days_ago, minutes=mins_offset)

        wstart = ts
        wend = ts + timedelta(minutes=1)

        rule = random.choice(["failed_login_burst", "blacklisted_ip"])

        if rule == "blacklisted_ip":
            ip = random.choice(BLACKLIST)
            cnt = random.randint(1, 6)
            sev = "high"
        else:
            ip = random.choice(NORMAL_IPS)
            cnt = random.randint(6, 25)
            sev = "high" if cnt > 12 else "medium"

        cur.execute(
            """
            INSERT INTO alerts
            (window_start, window_end, ip, rule, count, severity, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (wstart, wend, ip, rule, cnt, sev, wstart),
        )

    conn.commit()

    cur.execute("SELECT count(*) FROM alerts;")
    a = cur.fetchone()[0]

    cur.execute("SELECT count(*) FROM event_counts;")
    c = cur.fetchone()[0]

    print(f"[seed] done: {a} alerts, {c} event_counts rows inserted.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()