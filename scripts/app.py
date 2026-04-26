import os
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
from datetime import date, datetime

import pymysql
from flask import Flask, flash, jsonify, redirect, render_template, request, url_for


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, "templates"),
    static_folder=os.path.join(BASE_DIR, "static"),
)
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.secret_key = os.getenv("SECRET_KEY", "fraud-detection-engine-secret")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", ""),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "fraud_engine"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": False,
    "connect_timeout": 5,
}


@contextmanager
def db_connection():
    connection = pymysql.connect(**DB_CONFIG)
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def fetch_all(query, params=None):
    with db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchall()


def fetch_one(query, params=None):
    with db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchone()


def execute_write(query, params=None):
    with db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params or ())
            return cursor.lastrowid


def serialize_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, Decimal):
        return float(value)
    return value


def serialize_rows(rows):
    return [{key: serialize_value(value) for key, value in row.items()} for row in rows]


def validate_option(value, allowed, default):
    return value if value in allowed else default


def dashboard_summary():
    return fetch_one(
        """
        SELECT
            (SELECT COUNT(*) FROM users) AS total_users,
            (SELECT COUNT(*) FROM accounts) AS total_accounts,
            (SELECT COUNT(*) FROM devices) AS total_devices,
            (SELECT COUNT(*) FROM transactions) AS total_transactions,
            (SELECT COUNT(*) FROM fraud_alerts) AS total_alerts,
            (SELECT COUNT(*) FROM fraud_alerts WHERE status = 'open') AS open_alerts,
            (SELECT COUNT(*) FROM fraud_alerts WHERE status = 'resolved') AS resolved_alerts,
            (SELECT COUNT(*) FROM users WHERE overall_risk_score >= 70) AS high_risk_users
        """
    ) or {}


def recent_alert_details(limit=8, severity="all", status="all"):
    query = """
        SELECT
            fa.alert_id,
            fa.txn_id,
            fa.rule_id,
            fa.alert_timestamp,
            fa.severity,
            fa.status,
            u.user_id,
            u.name AS user_name,
            u.email AS user_email,
            a.account_id,
            a.account_type,
            a.status AS account_status,
            t.device_id,
            d.device_type,
            t.amount,
            t.transaction_type,
            t.location,
            t.ip_address,
            t.`timestamp` AS transaction_timestamp,
            r.rule_name,
            r.threshold_value,
            r.time_window_minutes,
            CASE
                WHEN r.rule_name = 'High Amount Spike' THEN CONCAT('Transaction amount ₹', FORMAT(t.amount, 0), ' exceeded threshold ₹', FORMAT(r.threshold_value, 0))
                WHEN r.rule_name = 'Rapid Transaction Burst' THEN CONCAT('More than ', r.threshold_value, ' transactions within ', r.time_window_minutes, ' minutes were detected for this account.')
                WHEN r.rule_name = 'Device Hopping' THEN CONCAT('More than ', r.threshold_value, ' unique devices were seen within ', r.time_window_minutes, ' minutes for this account.')
                ELSE r.rule_description
            END AS explanation,
            CASE
                WHEN r.rule_name = 'High Amount Spike' THEN CONCAT('Threshold ₹', FORMAT(r.threshold_value, 0))
                WHEN r.rule_name = 'Rapid Transaction Burst' THEN CONCAT(r.threshold_value, ' transactions / ', r.time_window_minutes, ' minutes')
                WHEN r.rule_name = 'Device Hopping' THEN CONCAT(r.threshold_value, ' devices / ', r.time_window_minutes, ' minutes')
                ELSE COALESCE(CAST(r.threshold_value AS CHAR), 'N/A')
            END AS threshold_summary
        FROM fraud_alerts fa
        JOIN transactions t ON fa.txn_id = t.txn_id
        JOIN accounts a ON t.account_id = a.account_id
        JOIN users u ON a.user_id = u.user_id
        JOIN devices d ON t.device_id = d.device_id
        JOIN fraud_rules r ON fa.rule_id = r.rule_id
        WHERE (%s = 'all' OR fa.severity = %s)
          AND (%s = 'all' OR fa.status = %s)
        ORDER BY fa.alert_timestamp DESC, fa.alert_id DESC
        LIMIT %s
    """
    return fetch_all(query, (severity, severity, status, status, limit))


def recent_suspicious_transactions(limit=8, suspicious_only=True):
    query = """
        SELECT *
        FROM (
            SELECT
                t.txn_id,
                t.account_id,
                u.user_id,
                u.name AS user_name,
                u.email AS user_email,
                a.account_type,
                a.status AS account_status,
                t.device_id,
                d.device_type,
                t.amount,
                t.transaction_type,
                t.location,
                t.ip_address,
                t.`timestamp` AS transaction_timestamp,
                t.status,
                COALESCE(alerts.alert_count, 0) AS alert_count,
                COALESCE(alerts.rules_triggered, '') AS rules_triggered,
                COALESCE(alerts.latest_alert_timestamp, t.`timestamp`) AS latest_alert_timestamp
            FROM transactions t
            JOIN accounts a ON t.account_id = a.account_id
            JOIN users u ON a.user_id = u.user_id
            JOIN devices d ON t.device_id = d.device_id
            LEFT JOIN (
                SELECT
                    fa.txn_id,
                    COUNT(*) AS alert_count,
                    GROUP_CONCAT(DISTINCT r.rule_name ORDER BY r.rule_name SEPARATOR ', ') AS rules_triggered,
                    MAX(fa.alert_timestamp) AS latest_alert_timestamp
                FROM fraud_alerts fa
                JOIN fraud_rules r ON fa.rule_id = r.rule_id
                GROUP BY fa.txn_id
            ) alerts ON t.txn_id = alerts.txn_id
        ) txn_view
        WHERE (%s = 0 OR alert_count > 0)
        ORDER BY latest_alert_timestamp DESC, transaction_timestamp DESC, txn_id DESC
        LIMIT %s
    """
    return fetch_all(query, (0 if suspicious_only else 1, limit))


def top_rules_triggered(limit=5):
    return fetch_all(
        """
        SELECT
            r.rule_id,
            r.rule_name,
            r.severity_level,
            r.threshold_value,
            r.time_window_minutes,
            COUNT(fa.alert_id) AS trigger_count,
            MAX(fa.alert_timestamp) AS last_triggered_at
        FROM fraud_rules r
        LEFT JOIN fraud_alerts fa ON r.rule_id = fa.rule_id
        GROUP BY r.rule_id, r.rule_name, r.severity_level, r.threshold_value, r.time_window_minutes
        ORDER BY trigger_count DESC, last_triggered_at DESC, rule_id ASC
        LIMIT %s
        """,
        (limit,),
    )


def high_risk_users(limit=10):
    return fetch_all(
        """
        SELECT
            u.user_id,
            u.name,
            u.email,
            u.phone,
            u.account_created_at,
            u.overall_risk_score,
            COUNT(DISTINCT a.account_id) AS account_count,
            COUNT(DISTINCT t.txn_id) AS transaction_count,
            COUNT(DISTINCT fa.alert_id) AS alert_count
        FROM users u
        LEFT JOIN accounts a ON u.user_id = a.user_id
        LEFT JOIN transactions t ON a.account_id = t.account_id
        LEFT JOIN fraud_alerts fa ON t.txn_id = fa.txn_id
        GROUP BY u.user_id, u.name, u.email, u.phone, u.account_created_at, u.overall_risk_score
        ORDER BY overall_risk_score DESC, alert_count DESC, transaction_count DESC, user_id ASC
        LIMIT %s
        """,
        (limit,),
    )


def scenario_snapshot(rule_name):
    return fetch_one(
        """
        SELECT
            fa.alert_id,
            fa.txn_id,
            fa.alert_timestamp,
            fa.severity,
            fa.status,
            u.name AS user_name,
            u.email AS user_email,
            r.rule_name,
            r.threshold_value,
            r.time_window_minutes,
            CASE
                WHEN r.rule_name = 'High Amount Spike' THEN CONCAT('Transaction amount ₹', FORMAT(t.amount, 0), ' exceeded threshold ₹', FORMAT(r.threshold_value, 0))
                WHEN r.rule_name = 'Rapid Transaction Burst' THEN CONCAT('More than ', r.threshold_value, ' transactions within ', r.time_window_minutes, ' minutes were detected for this account.')
                WHEN r.rule_name = 'Device Hopping' THEN CONCAT('More than ', r.threshold_value, ' unique devices were seen within ', r.time_window_minutes, ' minutes for this account.')
                ELSE r.rule_description
            END AS explanation,
            CASE
                WHEN r.rule_name = 'High Amount Spike' THEN CONCAT('Threshold ₹', FORMAT(r.threshold_value, 0))
                WHEN r.rule_name = 'Rapid Transaction Burst' THEN CONCAT(r.threshold_value, ' transactions / ', r.time_window_minutes, ' minutes')
                WHEN r.rule_name = 'Device Hopping' THEN CONCAT(r.threshold_value, ' devices / ', r.time_window_minutes, ' minutes')
                ELSE COALESCE(CAST(r.threshold_value AS CHAR), 'N/A')
            END AS threshold_summary,
            t.amount
        FROM fraud_alerts fa
        JOIN transactions t ON fa.txn_id = t.txn_id
        JOIN accounts a ON t.account_id = a.account_id
        JOIN users u ON a.user_id = u.user_id
        JOIN fraud_rules r ON fa.rule_id = r.rule_id
        WHERE r.rule_name = %s
        ORDER BY alert_timestamp DESC, alert_id DESC
        LIMIT 1
        """,
        (rule_name,),
    )


@app.route("/")
def dashboard():
    summary = dashboard_summary()
    recent_alerts = recent_alert_details(limit=6)
    suspicious_transactions = recent_suspicious_transactions(limit=8, suspicious_only=True)
    rules = top_rules_triggered(limit=5)
    risky_users = high_risk_users(limit=10)
    scenarios = [
        {"label": "Scenario 1", "title": "High-Value Spike", "alert": scenario_snapshot("High Amount Spike")},
        {"label": "Scenario 2", "title": "Rapid Burst Behaviour", "alert": scenario_snapshot("Rapid Transaction Burst")},
        {"label": "Scenario 3", "title": "Device Hopping", "alert": scenario_snapshot("Device Hopping")},
    ]

    return render_template(
        "dashboard.html",
        active_page="dashboard",
        page_title="Dashboard",
        summary=summary,
        recent_alerts=recent_alerts,
        suspicious_transactions=suspicious_transactions,
        rules=rules,
        risky_users=risky_users,
        scenarios=scenarios,
    )


@app.route("/transactions")
def transactions():
    view_mode = validate_option(request.args.get("view", "all"), {"all", "suspicious"}, "all")
    transactions_data = fetch_all(
        """
        SELECT *
        FROM (
            SELECT
                t.txn_id,
                t.account_id,
                u.user_id,
                u.name AS user_name,
                u.email AS user_email,
                a.account_type,
                a.status AS account_status,
                t.device_id,
                d.device_type,
                t.amount,
                t.transaction_type,
                t.location,
                t.ip_address,
                t.`timestamp` AS transaction_timestamp,
                t.status,
                COALESCE(alerts.alert_count, 0) AS alert_count,
                COALESCE(alerts.rules_triggered, '') AS rules_triggered,
                COALESCE(alerts.latest_alert_timestamp, t.`timestamp`) AS latest_alert_timestamp
            FROM transactions t
            JOIN accounts a ON t.account_id = a.account_id
            JOIN users u ON a.user_id = u.user_id
            JOIN devices d ON t.device_id = d.device_id
            LEFT JOIN (
                SELECT
                    fa.txn_id,
                    COUNT(*) AS alert_count,
                    GROUP_CONCAT(DISTINCT r.rule_name ORDER BY r.rule_name SEPARATOR ', ') AS rules_triggered,
                    MAX(fa.alert_timestamp) AS latest_alert_timestamp
                FROM fraud_alerts fa
                JOIN fraud_rules r ON fa.rule_id = r.rule_id
                GROUP BY fa.txn_id
            ) alerts ON t.txn_id = alerts.txn_id
        ) txn_view
        WHERE (%s = 'all' OR alert_count > 0)
        ORDER BY latest_alert_timestamp DESC, transaction_timestamp DESC, txn_id DESC
        """,
        (view_mode,),
    )

    accounts = fetch_all(
        """
        SELECT
            a.account_id,
            u.name AS user_name,
            a.account_type,
            a.status AS account_status,
            u.email AS user_email
        FROM accounts a
        JOIN users u ON a.user_id = u.user_id
        ORDER BY account_id ASC
        """
    )

    devices = fetch_all(
        """
        SELECT device_id, device_type, first_seen_at
        FROM devices
        ORDER BY first_seen_at DESC, device_id ASC
        """
    )

    return render_template(
        "transactions.html",
        active_page="transactions",
        page_title="Transactions",
        transactions=transactions_data,
        accounts=accounts,
        devices=devices,
        view_mode=view_mode,
        suspicious_count=sum(1 for txn in transactions_data if txn["alert_count"] > 0),
    )


@app.route("/add_transaction", methods=["POST"])
def add_transaction():
    account_id = request.form.get("account_id", "").strip()
    device_id = request.form.get("device_id", "").strip()
    amount_raw = request.form.get("amount", "").strip()
    transaction_type = request.form.get("transaction_type", "").strip()
    location = request.form.get("location", "").strip()
    ip_address = request.form.get("ip_address", "").strip()

    if not all([account_id, device_id, amount_raw, transaction_type, location, ip_address]):
        flash("All transaction fields are required.", "error")
        return redirect(url_for("transactions"))

    try:
        amount = Decimal(amount_raw)
    except InvalidOperation:
        flash("Amount must be a valid number.", "error")
        return redirect(url_for("transactions"))

    if amount <= 0:
        flash("Amount must be greater than zero.", "error")
        return redirect(url_for("transactions"))

    try:
        execute_write(
            """
            INSERT INTO transactions (
                account_id,
                device_id,
                amount,
                transaction_type,
                location,
                ip_address
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (int(account_id), device_id, amount, transaction_type, location, ip_address),
        )
        flash("Transaction inserted. SQL triggers will create any fraud alerts.", "success")
    except pymysql.MySQLError as exc:
        flash(f"Database error while inserting the transaction: {exc}", "error")

    return redirect(url_for("transactions"))


@app.route("/alerts")
def alerts():
    severity_filter = validate_option(request.args.get("severity", "all"), {"all", "high", "medium", "low"}, "all")
    status_filter = validate_option(request.args.get("status", "all"), {"all", "open", "resolved"}, "all")
    alerts_data = recent_alert_details(limit=1000, severity=severity_filter, status=status_filter)

    return render_template(
        "alerts.html",
        active_page="alerts",
        page_title="Alerts",
        alerts=alerts_data,
        severity_filter=severity_filter,
        status_filter=status_filter,
    )


@app.route("/alerts/<int:alert_id>/toggle-status", methods=["POST"])
def toggle_alert_status(alert_id):
    severity_filter = validate_option(request.form.get("severity", "all"), {"all", "high", "medium", "low"}, "all")
    status_filter = validate_option(request.form.get("status", "all"), {"all", "open", "resolved"}, "all")

    execute_write(
        """
        UPDATE fraud_alerts
        SET status = CASE WHEN status = 'open' THEN 'resolved' ELSE 'open' END
        WHERE alert_id = %s
        """,
        (alert_id,),
    )
    flash("Alert status updated.", "success")
    return redirect(url_for("alerts", severity=severity_filter, status=status_filter))


@app.route("/users")
def users():
    sort_mode = validate_option(request.args.get("sort", "desc"), {"desc", "asc"}, "desc")
    users_data = fetch_all(
        f"""
        SELECT
            u.user_id,
            u.name,
            u.email,
            u.phone,
            u.account_created_at,
            u.overall_risk_score,
            COUNT(DISTINCT a.account_id) AS account_count,
            COUNT(DISTINCT t.txn_id) AS transaction_count,
            COUNT(DISTINCT fa.alert_id) AS alert_count
        FROM users u
        LEFT JOIN accounts a ON u.user_id = a.user_id
        LEFT JOIN transactions t ON a.account_id = t.account_id
        LEFT JOIN fraud_alerts fa ON t.txn_id = fa.txn_id
        GROUP BY u.user_id, u.name, u.email, u.phone, u.account_created_at, u.overall_risk_score
        ORDER BY u.overall_risk_score {sort_mode.upper()}, alert_count DESC, transaction_count DESC, u.user_id ASC
        """
    )
    top_risk_ids = {row["user_id"] for row in fetch_all("SELECT user_id FROM users ORDER BY overall_risk_score DESC, user_id ASC LIMIT 10")}

    return render_template(
        "users.html",
        active_page="users",
        page_title="Users",
        users=users_data,
        sort_mode=sort_mode,
        top_risk_ids=top_risk_ids,
    )


@app.route("/api/summary")
def api_summary():
    return jsonify({key: serialize_value(value) for key, value in dashboard_summary().items()})


@app.route("/api/recent-alerts")
def api_recent_alerts():
    return jsonify(alerts=serialize_rows(recent_suspicious_transactions(limit=8, suspicious_only=True)))


@app.route("/api/alerts")
def api_alerts():
    severity_filter = validate_option(request.args.get("severity", "all"), {"all", "high", "medium", "low"}, "all")
    status_filter = validate_option(request.args.get("status", "all"), {"all", "open", "resolved"}, "all")
    return jsonify(alerts=serialize_rows(recent_alert_details(limit=1000, severity=severity_filter, status=status_filter)))


@app.route("/api/rules")
def api_rules():
    return jsonify(rules=serialize_rows(top_rules_triggered(limit=5)))


if __name__ == "__main__":
    app.run(
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "5000")),
        debug=True,
    )
