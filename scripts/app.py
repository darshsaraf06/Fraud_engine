from flask import Flask, render_template, request, redirect
import pymysql

app = Flask(__name__)

def get_db_connection():
    return pymysql.connect(
        host="localhost",
        user="appuser",
        password="app123",
        database="fraud_engine",
        cursorclass=pymysql.cursors.DictCursor
    )

@app.route("/")
def dashboard():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM users")
    users = cursor.fetchall()

    cursor.execute("SELECT * FROM fraud_alerts")
    alerts = cursor.fetchall()

    conn.close()
    return render_template("dashboard.html", users=users, alerts=alerts)


@app.route("/transactions")
def transactions():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Fetch transactions
    cursor.execute("""
        SELECT t.*, a.account_type 
        FROM transactions t
        JOIN accounts a ON t.account_id = a.account_id
        ORDER BY t.timestamp DESC
    """)
    transactions = cursor.fetchall()

    # Fetch accounts for dropdown
    cursor.execute("""
        SELECT account_id, account_type 
        FROM accounts
    """)
    accounts = cursor.fetchall()

    # Fetch devices for dropdown
    cursor.execute("""
        SELECT device_id, device_type 
        FROM devices
    """)
    devices = cursor.fetchall()

    conn.close()

    return render_template(
        "transactions.html",
        transactions=transactions,
        accounts=accounts,
        devices=devices
    )


@app.route("/add_transaction", methods=["POST"])
def add_transaction():
    try:
        account_id = request.form["account_id"]
        device_id = request.form["device_id"]
        amount = request.form["amount"]
        transaction_type = request.form["transaction_type"]
        location = request.form["location"]
        ip_address = request.form["ip_address"]

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO transactions 
            (account_id, device_id, amount, transaction_type, location, ip_address)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (account_id, device_id, amount, transaction_type, location, ip_address))

        conn.commit()
        print("Transaction inserted successfully")

        conn.close()

    except Exception as e:
        print("ERROR:", e)

    return redirect("/transactions")


if __name__ == "__main__":
    app.run(debug=True)
