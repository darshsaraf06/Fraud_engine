const mysql = require("mysql2");

const db = mysql.createConnection({
  host: "localhost",
  user: "fraud_user",       // use your created user
  password: "StrongPassword123",
  database: "fraud_engine"
});

db.connect((err) => {
  if (err) {
    console.error("Database connection failed:", err);
    return;
  }
  console.log("Connected to MySQL Database");
});

module.exports = db;