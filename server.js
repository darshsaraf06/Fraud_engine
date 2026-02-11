const express = require("express");
const mysql = require("mysql2");
const cors = require("cors");
const bodyParser = require("body-parser");

const app = express();
app.use(cors());
app.use(bodyParser.json());

// MySQL Connection
const db = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "Ishitamom@2",
  database: "fraud_engine"
});

db.connect(err => {
  if (err) {
    console.error("DB connection failed:", err);
    return;
  }
  console.log("Connected to MySQL");
});

// Insert Transaction
app.post("/add-transaction", (req, res) => {
  const { account_id, amount, device_id } = req.body;

  const deviceQuery = `
    INSERT INTO devices (device_id, device_type)
    VALUES (?, 'unknown')
    ON DUPLICATE KEY UPDATE device_id = device_id;
  `;

  db.query(deviceQuery, [device_id], err => {
    if (err) return res.status(500).json(err);

    const txnQuery = `
      INSERT INTO transactions (account_id, amount, device_id)
      VALUES (?, ?, ?);
    `;

    db.query(txnQuery, [account_id, amount, device_id], (err, result) => {
      if (err) return res.status(500).json(err);

      res.json({
        message: "Transaction added successfully",
        transaction_id: result.insertId
      });
    });
  });
});

// Fetch Fraud Alerts
app.get("/alerts", (req, res) => {
  const query = `
    SELECT fa.alert_id, fa.txn_id, fr.rule_name, fa.severity, fa.status, fa.alert_timestamp
    FROM fraud_alerts fa
    JOIN fraud_rules fr ON fa.rule_id = fr.rule_id
    ORDER BY fa.alert_timestamp DESC;
  `;

  db.query(query, (err, results) => {
    if (err) return res.status(500).json(err);
    res.json(results);
  });
});

app.listen(5000, () => {
  console.log("Server running on http://localhost:5000");
});