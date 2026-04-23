from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "database" / "database.sql"
SEED = 20260414

USER_COUNT = 1000
ACCOUNT_COUNT = 1800
DEVICE_COUNT = 3200
TRANSACTION_COUNT = 7200

FIRST_NAMES = [
    "Aarav", "Aditi", "Aditya", "Akash", "Ananya", "Arjun", "Avni", "Bhavya", "Dev", "Diya",
    "Ishaan", "Isha", "Kabir", "Kavya", "Krishna", "Meera", "Mira", "Myra", "Neha", "Nikhil",
    "Nitya", "Pooja", "Pranav", "Priya", "Rahul", "Riya", "Rohan", "Saanvi", "Sakshi", "Sanjay",
    "Sara", "Shreya", "Siddharth", "Sneha", "Suhana", "Tanya", "Uday", "Varun", "Vikram", "Zoya",
]

LAST_NAMES = [
    "Agrawal", "Bhatia", "Chatterjee", "Das", "Gupta", "Iyer", "Jain", "Kapoor", "Khan", "Khanna",
    "Kulkarni", "Mehta", "Nair", "Patel", "Rao", "Saxena", "Sharma", "Shetty", "Singh", "Verma",
    "Agarwal", "Bose", "Choudhary", "Dutta", "Ghosh", "Malhotra", "Menon", "Mishra", "Sethi", "Yadav",
]

EMAIL_DOMAINS = [
    "gmail.com", "outlook.com", "yahoo.in", "rediffmail.com", "icloud.com", "proton.me",
    "company.in", "finmail.in",
]

ACCOUNT_TYPES = ["savings", "current", "salary", "business", "joint"]
ACCOUNT_STATUS = ["active", "active", "active", "frozen", "dormant"]
DEVICE_TYPES = ["mobile", "mobile", "mobile", "laptop", "desktop", "tablet"]
TXN_TYPES = ["debit", "debit", "credit"]
LOCATIONS = [
    "Delhi", "Mumbai", "Bengaluru", "Hyderabad", "Chennai", "Pune", "Kolkata", "Ahmedabad",
    "Jaipur", "Lucknow", "Surat", "Chandigarh", "Kochi", "Indore", "Bhopal", "Noida",
    "Gurugram", "Nagpur", "Visakhapatnam", "Vadodara",
]
IP_POOLS = [
    "10", "10", "10", "172", "172", "192", "103", "49", "14"
]


class Raw(str):
    pass


def raw(value: str) -> Raw:
    return Raw(value)


def q(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, Raw):
        return str(value)
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, datetime):
        return "'" + value.strftime("%Y-%m-%d %H:%M:%S") + "'"
    return "'" + str(value).replace("'", "''") + "'"


def row(values) -> str:
    return "(" + ", ".join(q(v) for v in values) + ")"


def batched_insert(table: str, columns: list[str], rows: list[list], batch_size: int = 250) -> list[str]:
    statements = []
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        statements.append(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n" + ",\n".join(row(r) for r in batch) + ";"
        )
    return statements


def make_datetime(days_back: int, hour: int, minute: int, second: int = 0) -> datetime:
    base = datetime(2026, 4, 14, 16, 0, 0)
    return base - timedelta(days=days_back, hours=hour, minutes=minute, seconds=second)


def build_users() -> list[list]:
    combos = [(f, l) for f in FIRST_NAMES for l in LAST_NAMES]
    random.shuffle(combos)
    users = []
    for idx in range(USER_COUNT):
        first, last = combos[idx]
        name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}{idx + 1}@{random.choice(EMAIL_DOMAINS)}"
        phone = f"{random.choice(['7', '8', '9'])}{random.randint(100000000, 999999999)}"
        created_at = make_datetime(random.randint(30, 1400), random.randint(0, 23), random.randint(0, 59))
        base_risk = round(random.uniform(1, 36), 2)
        users.append([name, email, phone, created_at, base_risk])
    return users


def build_accounts() -> list[list]:
    rows = []
    for user_id in range(1, USER_COUNT + 1):
        account_total = 2 if user_id <= 800 else 1
        for slot in range(account_total):
            account_type = ACCOUNT_TYPES[(user_id + slot) % len(ACCOUNT_TYPES)]
            status = random.choice(ACCOUNT_STATUS)
            created_at = make_datetime(random.randint(5, 1200), random.randint(0, 23), random.randint(0, 59))
            rows.append([user_id, account_type, status, created_at])
    return rows[:ACCOUNT_COUNT]


def build_devices() -> tuple[list[list], list[str]]:
    rows = []
    ids = []
    for idx in range(1, DEVICE_COUNT + 1):
        device_type = random.choice(DEVICE_TYPES)
        if device_type == "mobile":
            device_id = f"DEV-MOB-{idx:05d}"
        elif device_type == "laptop":
            device_id = f"DEV-LAP-{idx:05d}"
        elif device_type == "tablet":
            device_id = f"DEV-TAB-{idx:05d}"
        else:
            device_id = f"DEV-DES-{idx:05d}"
        first_seen = make_datetime(random.randint(1, 1000), random.randint(0, 23), random.randint(0, 59))
        rows.append([device_id, device_type, first_seen])
        ids.append(device_id)
    return rows, ids


def build_rules() -> list[list]:
    return [
        ["High Amount Spike", "Triggers when the transaction amount exceeds the threshold.", 10000, None, "high", True],
        ["Rapid Transaction Burst", "Triggers when many transactions occur in a short window.", 5, 10, "medium", True],
        ["Device Hopping", "Triggers when too many devices are seen within the window.", 3, 15, "high", True],
    ]


def random_ip() -> str:
    pool = random.choice(IP_POOLS)
    if pool == "10":
        return f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
    if pool == "172":
        return f"172.16.{random.randint(0, 255)}.{random.randint(1, 254)}"
    if pool == "192":
        return f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}"
    return f"{pool}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"


def random_amount() -> float:
    roll = random.random()
    if roll < 0.72:
        return round(random.uniform(120, 4200), 2)
    if roll < 0.92:
        return round(random.uniform(4200, 9800), 2)
    if roll < 0.98:
        return round(random.uniform(10050, 24000), 2)
    return round(random.uniform(24000, 48000), 2)


def build_transactions(device_ids: list[str]) -> list[list]:
    rows = []
    bulk_count = TRANSACTION_COUNT - 19
    start_time = datetime(2024, 1, 1, 8, 0, 0)
    span_minutes = 780 * 24 * 60

    for idx in range(bulk_count):
        account_id = random.randint(1, ACCOUNT_COUNT)
        device_id = random.choice(device_ids)
        amount = random_amount()
        txn_type = random.choice(TXN_TYPES)
        location = random.choice(LOCATIONS)
        ip_address = random_ip()
        txn_time = start_time + timedelta(minutes=random.randint(0, span_minutes), seconds=random.randint(0, 59))
        status = random.choices(["completed", "completed", "completed", "pending", "reversed"], weights=[72, 18, 4, 4, 2])[0]
        rows.append([account_id, device_id, amount, txn_type, location, ip_address, txn_time, status])

    scenario1_account = 5
    scenario1_device = device_ids[0]
    for offset in range(4):
        rows.append([
            scenario1_account,
            scenario1_device,
            round(random.uniform(250, 1800), 2),
            "debit",
            "Delhi",
            "10.10.1.10",
            raw(f"NOW() - INTERVAL {25 - offset} MINUTE"),
            "completed",
        ])
    rows.append([
        scenario1_account,
        scenario1_device,
        50000.00,
        "debit",
        "Delhi",
        "10.10.1.11",
        raw("NOW() - INTERVAL 1 MINUTE"),
        "completed",
    ])

    scenario2_account = 12
    scenario2_device = device_ids[1]
    for offset in range(10):
        rows.append([
            scenario2_account,
            scenario2_device,
            round(random.uniform(180, 650), 2),
            "debit",
            "Mumbai",
            f"172.16.8.{40 + offset}",
            raw(f"NOW() - INTERVAL {9 - offset} MINUTE"),
            "completed",
        ])

    scenario3_account = 28
    devices = device_ids[2:6]
    for offset, device_id in enumerate(devices):
        rows.append([
            scenario3_account,
            device_id,
            round(random.uniform(300, 1400), 2),
            random.choice(["debit", "credit"]),
            "Bengaluru",
            f"192.168.20.{100 + offset}",
            raw(f"NOW() - INTERVAL {4 - offset} MINUTE"),
            "completed",
        ])

    return rows


def build_sql() -> str:
    random.seed(SEED)
    users_rows = build_users()
    accounts_rows = build_accounts()
    device_rows, device_ids = build_devices()
    rules_rows = build_rules()
    transaction_rows = build_transactions(device_ids)

    parts = [
        "DROP DATABASE IF EXISTS fraud_engine;",
        "CREATE DATABASE fraud_engine;",
        "USE fraud_engine;",
        "",
        "CREATE TABLE users (",
        "    user_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,",
        "    name VARCHAR(100) NOT NULL,",
        "    email VARCHAR(150) NOT NULL UNIQUE,",
        "    phone VARCHAR(20),",
        "    account_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,",
        "    overall_risk_score DECIMAL(10,2) NOT NULL DEFAULT 0",
        ") ENGINE=InnoDB;",
        "",
        "CREATE TABLE accounts (",
        "    account_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,",
        "    user_id INT UNSIGNED NOT NULL,",
        "    account_type VARCHAR(50) NOT NULL,",
        "    status VARCHAR(20) NOT NULL DEFAULT 'active',",
        "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,",
        "    CONSTRAINT fk_accounts_user FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE",
        ") ENGINE=InnoDB;",
        "",
        "CREATE TABLE devices (",
        "    device_id VARCHAR(100) PRIMARY KEY,",
        "    device_type VARCHAR(50) NOT NULL,",
        "    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        ") ENGINE=InnoDB;",
        "",
        "CREATE TABLE transactions (",
        "    txn_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,",
        "    account_id INT UNSIGNED NOT NULL,",
        "    device_id VARCHAR(100) NOT NULL,",
        "    amount DECIMAL(12,2) NOT NULL,",
        "    transaction_type VARCHAR(20) NOT NULL,",
        "    location VARCHAR(100) NOT NULL,",
        "    ip_address VARCHAR(50) NOT NULL,",
        "    `timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,",
        "    status VARCHAR(20) NOT NULL DEFAULT 'completed',",
        "    CONSTRAINT chk_positive_amount CHECK (amount > 0),",
        "    CONSTRAINT fk_transactions_account FOREIGN KEY (account_id) REFERENCES accounts(account_id) ON DELETE CASCADE,",
        "    CONSTRAINT fk_transactions_device FOREIGN KEY (device_id) REFERENCES devices(device_id)",
        ") ENGINE=InnoDB;",
        "",
        "CREATE TABLE fraud_rules (",
        "    rule_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,",
        "    rule_name VARCHAR(100) NOT NULL UNIQUE,",
        "    rule_description TEXT NOT NULL,",
        "    threshold_value DECIMAL(12,2),",
        "    time_window_minutes INT,",
        "    severity_level VARCHAR(20) NOT NULL,",
        "    is_active BOOLEAN NOT NULL DEFAULT TRUE,",
        "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        ") ENGINE=InnoDB;",
        "",
        "CREATE TABLE fraud_alerts (",
        "    alert_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,",
        "    txn_id BIGINT UNSIGNED NOT NULL,",
        "    rule_id INT UNSIGNED NOT NULL,",
        "    alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,",
        "    severity VARCHAR(20) NOT NULL,",
        "    status VARCHAR(20) NOT NULL DEFAULT 'open',",
        "    CONSTRAINT fk_alerts_transaction FOREIGN KEY (txn_id) REFERENCES transactions(txn_id) ON DELETE CASCADE,",
        "    CONSTRAINT fk_alerts_rule FOREIGN KEY (rule_id) REFERENCES fraud_rules(rule_id)",
        ") ENGINE=InnoDB;",
        "",
        "CREATE INDEX idx_transactions_account_timestamp ON transactions(account_id, `timestamp`);",
        "CREATE INDEX idx_transactions_device_timestamp ON transactions(device_id, `timestamp`);",
        "CREATE INDEX idx_fraud_alerts_rule ON fraud_alerts(rule_id);",
        "CREATE INDEX idx_fraud_alerts_status ON fraud_alerts(status);",
        "CREATE INDEX idx_users_risk_score ON users(overall_risk_score);",
        "",
        *batched_insert("users", ["name", "email", "phone", "account_created_at", "overall_risk_score"], users_rows, batch_size=250),
        "",
        *batched_insert("accounts", ["user_id", "account_type", "status", "created_at"], accounts_rows, batch_size=250),
        "",
        *batched_insert("devices", ["device_id", "device_type", "first_seen_at"], device_rows, batch_size=250),
        "",
        "INSERT INTO fraud_rules (rule_name, rule_description, threshold_value, time_window_minutes, severity_level, is_active) VALUES",
        ",\n".join(row(r) for r in rules_rows) + ";",
        "",
        "DELIMITER $$",
        "",
        "CREATE TRIGGER detect_fraud_after_insert",
        "AFTER INSERT ON transactions",
        "FOR EACH ROW",
        "BEGIN",
        "    DECLARE burst_threshold INT DEFAULT 5;",
        "    DECLARE burst_window INT DEFAULT 10;",
        "    DECLARE device_threshold INT DEFAULT 3;",
        "    DECLARE device_window INT DEFAULT 15;",
        "    DECLARE amount_threshold DECIMAL(12,2) DEFAULT 10000.00;",
        "    DECLARE txn_count INT DEFAULT 0;",
        "    DECLARE device_count INT DEFAULT 0;",
        "    DECLARE rule_id_value INT UNSIGNED;",
        "    DECLARE severity_value VARCHAR(20);",
        "",
        "    SELECT rule_id, severity_level INTO rule_id_value, severity_value",
        "    FROM fraud_rules",
        "    WHERE rule_name = 'High Amount Spike' AND is_active = TRUE",
        "    LIMIT 1;",
        "    IF NEW.amount > amount_threshold THEN",
        "        INSERT INTO fraud_alerts (txn_id, rule_id, severity, status)",
        "        VALUES (NEW.txn_id, rule_id_value, severity_value, 'open');",
        "    END IF;",
        "",
        "    SELECT rule_id, threshold_value, time_window_minutes, severity_level",
        "    INTO rule_id_value, burst_threshold, burst_window, severity_value",
        "    FROM fraud_rules",
        "    WHERE rule_name = 'Rapid Transaction Burst' AND is_active = TRUE",
        "    LIMIT 1;",
        "    SELECT COUNT(*) INTO txn_count",
        "    FROM transactions",
        "    WHERE account_id = NEW.account_id",
        "      AND `timestamp` >= NOW() - INTERVAL burst_window MINUTE;",
        "    IF txn_count > burst_threshold THEN",
        "        INSERT INTO fraud_alerts (txn_id, rule_id, severity, status)",
        "        VALUES (NEW.txn_id, rule_id_value, severity_value, 'open');",
        "    END IF;",
        "",
        "    SELECT rule_id, threshold_value, time_window_minutes, severity_level",
        "    INTO rule_id_value, device_threshold, device_window, severity_value",
        "    FROM fraud_rules",
        "    WHERE rule_name = 'Device Hopping' AND is_active = TRUE",
        "    LIMIT 1;",
        "    SELECT COUNT(DISTINCT device_id) INTO device_count",
        "    FROM transactions",
        "    WHERE account_id = NEW.account_id",
        "      AND `timestamp` >= NOW() - INTERVAL device_window MINUTE;",
        "    IF device_count > device_threshold THEN",
        "        INSERT INTO fraud_alerts (txn_id, rule_id, severity, status)",
        "        VALUES (NEW.txn_id, rule_id_value, severity_value, 'open');",
        "    END IF;",
        "END$$",
        "",
        "CREATE TRIGGER update_user_risk_after_alert",
        "AFTER INSERT ON fraud_alerts",
        "FOR EACH ROW",
        "BEGIN",
        "    UPDATE users u",
        "    JOIN accounts a ON u.user_id = a.user_id",
        "    JOIN transactions t ON a.account_id = t.account_id",
        "    SET u.overall_risk_score = LEAST(100, u.overall_risk_score + 12)",
        "    WHERE t.txn_id = NEW.txn_id;",
        "END$$",
        "",
        "CREATE TRIGGER prevent_delete_open_alerts",
        "BEFORE DELETE ON fraud_alerts",
        "FOR EACH ROW",
        "BEGIN",
        "    IF OLD.status = 'open' THEN",
        "        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Open fraud alerts cannot be deleted';",
        "    END IF;",
        "END$$",
        "",
        "DELIMITER ;",
        "",
        "CREATE VIEW vw_account_directory AS",
        "SELECT",
        "    a.account_id,",
        "    a.user_id,",
        "    u.name AS user_name,",
        "    u.email,",
        "    a.account_type,",
        "    a.status AS account_status,",
        "    a.created_at",
        "FROM accounts a",
        "JOIN users u ON a.user_id = u.user_id;",
        "",
        "CREATE VIEW vw_rule_trigger_summary AS",
        "SELECT",
        "    r.rule_id,",
        "    r.rule_name,",
        "    r.severity_level,",
        "    r.threshold_value,",
        "    r.time_window_minutes,",
        "    COUNT(fa.alert_id) AS trigger_count,",
        "    MAX(fa.alert_timestamp) AS last_triggered_at",
        "FROM fraud_rules r",
        "LEFT JOIN fraud_alerts fa ON r.rule_id = fa.rule_id",
        "GROUP BY r.rule_id, r.rule_name, r.severity_level, r.threshold_value, r.time_window_minutes;",
        "",
        "CREATE VIEW vw_transaction_overview AS",
        "SELECT",
        "    t.txn_id,",
        "    t.account_id,",
        "    ad.user_id,",
        "    ad.user_name,",
        "    ad.email AS user_email,",
        "    ad.account_type,",
        "    ad.account_status,",
        "    t.device_id,",
        "    d.device_type,",
        "    t.amount,",
        "    t.transaction_type,",
        "    t.location,",
        "    t.ip_address,",
        "    t.`timestamp` AS transaction_timestamp,",
        "    t.status,",
        "    COALESCE(alerts.alert_count, 0) AS alert_count,",
        "    COALESCE(alerts.rules_triggered, '') AS rules_triggered,",
        "    COALESCE(alerts.latest_alert_timestamp, t.`timestamp`) AS latest_alert_timestamp",
        "FROM transactions t",
        "JOIN devices d ON t.device_id = d.device_id",
        "JOIN vw_account_directory ad ON t.account_id = ad.account_id",
        "LEFT JOIN (",
        "    SELECT",
        "        fa.txn_id,",
        "        COUNT(*) AS alert_count,",
        "        GROUP_CONCAT(DISTINCT r.rule_name ORDER BY r.rule_name SEPARATOR ', ') AS rules_triggered,",
        "        MAX(fa.alert_timestamp) AS latest_alert_timestamp",
        "    FROM fraud_alerts fa",
        "    JOIN fraud_rules r ON fa.rule_id = r.rule_id",
        "    GROUP BY fa.txn_id",
        ") alerts ON t.txn_id = alerts.txn_id;",
        "",
        "CREATE VIEW vw_alert_details AS",
        "SELECT",
        "    fa.alert_id,",
        "    fa.txn_id,",
        "    fa.rule_id,",
        "    fa.alert_timestamp,",
        "    fa.severity,",
        "    fa.status,",
        "    t.account_id,",
        "    ad.user_id,",
        "    ad.user_name,",
        "    ad.email AS user_email,",
        "    t.device_id,",
        "    d.device_type,",
        "    t.amount,",
        "    t.transaction_type,",
        "    t.location,",
        "    t.ip_address,",
        "    t.`timestamp` AS transaction_timestamp,",
        "    r.rule_name,",
        "    r.threshold_value,",
        "    r.time_window_minutes,",
        "    CASE",
        "        WHEN r.rule_name = 'High Amount Spike' THEN CONCAT('Transaction amount ₹', FORMAT(t.amount, 0), ' exceeded threshold ₹', FORMAT(r.threshold_value, 0))",
        "        WHEN r.rule_name = 'Rapid Transaction Burst' THEN CONCAT('More than ', r.threshold_value, ' transactions within ', r.time_window_minutes, ' minutes were detected for this account.')",
        "        WHEN r.rule_name = 'Device Hopping' THEN CONCAT('More than ', r.threshold_value, ' unique devices were seen within ', r.time_window_minutes, ' minutes for this account.')",
        "        ELSE r.rule_description",
        "    END AS explanation,",
        "    CASE",
        "        WHEN r.rule_name = 'High Amount Spike' THEN CONCAT('Threshold ₹', FORMAT(r.threshold_value, 0))",
        "        WHEN r.rule_name = 'Rapid Transaction Burst' THEN CONCAT(r.threshold_value, ' transactions / ', r.time_window_minutes, ' minutes')",
        "        WHEN r.rule_name = 'Device Hopping' THEN CONCAT(r.threshold_value, ' devices / ', r.time_window_minutes, ' minutes')",
        "        ELSE COALESCE(CAST(r.threshold_value AS CHAR), 'N/A')",
        "    END AS threshold_summary",
        "FROM fraud_alerts fa",
        "JOIN transactions t ON fa.txn_id = t.txn_id",
        "JOIN devices d ON t.device_id = d.device_id",
        "JOIN vw_account_directory ad ON t.account_id = ad.account_id",
        "JOIN fraud_rules r ON fa.rule_id = r.rule_id;",
        "",
        "CREATE VIEW vw_user_risk_summary AS",
        "SELECT",
        "    u.user_id,",
        "    u.name,",
        "    u.email,",
        "    u.phone,",
        "    u.account_created_at,",
        "    u.overall_risk_score,",
        "    COUNT(DISTINCT a.account_id) AS account_count,",
        "    COUNT(DISTINCT t.txn_id) AS transaction_count,",
        "    COUNT(DISTINCT fa.alert_id) AS alert_count",
        "FROM users u",
        "LEFT JOIN accounts a ON u.user_id = a.user_id",
        "LEFT JOIN transactions t ON a.account_id = t.account_id",
        "LEFT JOIN fraud_alerts fa ON t.txn_id = fa.txn_id",
        "GROUP BY u.user_id, u.name, u.email, u.phone, u.account_created_at, u.overall_risk_score;",
        "",
    ]

    parts.extend(
        [
            *batched_insert(
                "transactions",
                ["account_id", "device_id", "amount", "transaction_type", "location", "ip_address", "`timestamp`", "status"],
                transaction_rows,
                batch_size=250,
            ),
            "",
            "UPDATE fraud_alerts SET status = 'resolved' WHERE alert_id % 7 = 0;",
        ]
    )
    return "\n".join(parts)


def main() -> None:
    OUTPUT.write_text(build_sql(), encoding="utf-8")
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
