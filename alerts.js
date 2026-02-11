const express = require("express");
const router = express.Router();
const db = require("../db");

// Get All Alerts
router.get("/", (req, res) => {
  const query = `
    SELECT fa.alert_id, fa.txn_id, fr.rule_name,
           fa.severity, fa.status, fa.alert_timestamp
    FROM fraud_alerts fa
    JOIN fraud_rules fr ON fa.rule_id = fr.rule_id
    ORDER BY fa.alert_timestamp DESC;
  `;

  db.query(query, (err, results) => {
    if (err) return res.status(500).json(err);
    res.json(results);
  });
});

module.exports = router;