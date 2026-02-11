const express = require("express");
const router = express.Router();
const db = require("../db");

// Add Transaction
router.post("/add", (req, res) => {
  const { account_id, amount, device_id } = req.body;

  const insertDevice = `
    INSERT INTO devices (device_id, device_type)
    VALUES (?, 'unknown')
    ON DUPLICATE KEY UPDATE device_id = device_id;
  `;

  db.query(insertDevice, [device_id], (err) => {
    if (err) return res.status(500).json(err);

    const insertTxn = `
      INSERT INTO transactions (account_id, amount, device_id)
      VALUES (?, ?, ?);
    `;

    db.query(insertTxn, [account_id, amount, device_id], (err, result) => {
      if (err) return res.status(500).json(err);

      res.json({
        message: "Transaction inserted",
        txn_id: result.insertId
      });
    });
  });
});

module.exports = router;