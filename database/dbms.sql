CREATE DATABASE fraud_engine;
use fraud_engine;

-- USERS
CREATE TABLE users (
    user_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    phone VARCHAR(15),
    account_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    overall_risk_score DECIMAL(10,2) DEFAULT 0
) ENGINE=InnoDB;

INSERT INTO users (name, email, phone, overall_risk_score) VALUES('Barry', 'barry@example.com', '9876543210', 10.00), ('Allen', 'allen@example.com', '9123456780', 5.00);
SELECT * FROM users;

-- Accounts
CREATE TABLE accounts (
    account_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNSIGNED,
    account_type VARCHAR(50),
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
) ENGINE=InnoDB;

INSERT INTO accounts (user_id, account_type, status) VALUES (1, 'savings', 'active'), (2, 'current', 'active');
SELECT * FROM accounts;

-- DEVICES
CREATE TABLE devices (
    device_id VARCHAR(100) PRIMARY KEY,
    device_type VARCHAR(50),
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO devices (device_id, device_type) VALUES ('device_mobile_01', 'mobile'), ('device_laptop_01', 'laptop');
SELECT * FROM devices;

-- TRANSACTIONS
CREATE TABLE transactions (
    txn_id SERIAL PRIMARY KEY,
    account_id INT UNSIGNED REFERENCES accounts(account_id) ON DELETE CASCADE,
    device_id VARCHAR(100) REFERENCES devices(device_id),
    amount NUMERIC(12,2) NOT NULL,
    transaction_type VARCHAR(20),
    location VARCHAR(100),
    ip_address VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'completed'
);

INSERT INTO transactions(account_id, device_id, amount, transaction_type, location, ip_address) VALUES (1, 'device_mobile_01', 15000.00, 'debit', 'Delhi', '192.168.1.10'), (2, 'device_laptop_01', 2500.00, 'credit', 'Mumbai', '192.168.1.20');
SELECT * FROM transactions;

-- FRAUD RULES
CREATE TABLE fraud_rules (
    rule_id SERIAL PRIMARY KEY,
    rule_name VARCHAR(100),
    rule_description TEXT,
    threshold_value NUMERIC,
    time_window_minutes INT,
    severity_level VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO fraud_rules(rule_name, rule_description, threshold_value, time_window_minutes, severity_level, is_active) VALUES ('High Amount Spike', 'Triggers when transaction exceeds threshold amount', 10000, NULL, 'high', TRUE), ('Rapid Transaction Burst',  'Triggers when multiple transactions occur in short time window', 5, 10, 'medium', TRUE);
SELECT * FROM fraud_rules;

-- FRAUD ALERTS
CREATE TABLE fraud_alerts (
    alert_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    txn_id INT UNSIGNED,
    rule_id INT UNSIGNED,
    alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    severity VARCHAR(20),
    status VARCHAR(20) DEFAULT 'open'
);

INSERT INTO fraud_alerts(txn_id, rule_id, severity, status) VALUES (1, 1, 'high', 'open'), (2, 2, 'medium', 'open');
SELECT * FROM fraud_alerts;

-- Initial Fraud Rules
INSERT INTO fraud_rules 
(rule_name, rule_description, threshold_value, time_window_minutes, severity_level)
VALUES
('Rapid Transaction Burst',
 'More than N transactions within time window',
 5, 10, 'high'),

('High Amount Spike',
 'Transaction amount greater than threshold',
 10000, NULL, 'medium'),

('Device Hopping',
 'Multiple devices used within time window',
 3, 15, 'high');

-- Trigger Implementation
DELIMITER $$

CREATE TRIGGER detect_fraud_after_insert
AFTER INSERT ON transactions
FOR EACH ROW
BEGIN

    DECLARE txn_count INT;
    DECLARE device_count INT;
    DECLARE burst_threshold INT;
    DECLARE burst_window INT;
    DECLARE device_threshold INT;
    DECLARE device_window INT;
    DECLARE amount_threshold DECIMAL(12,2);

    -- Get rule values dynamically
    SELECT threshold_value, time_window_minutes
    INTO burst_threshold, burst_window
    FROM fraud_rules
    WHERE rule_name = 'Rapid Transaction Burst' AND is_active = TRUE
    LIMIT 1;

    SELECT threshold_value
    INTO amount_threshold
    FROM fraud_rules
    WHERE rule_name = 'High Amount Spike' AND is_active = TRUE
    LIMIT 1;

    SELECT threshold_value, time_window_minutes
    INTO device_threshold, device_window
    FROM fraud_rules
    WHERE rule_name = 'Device Hopping' AND is_active = TRUE
    LIMIT 1;

    -- 1️⃣ Rapid Transaction Burst
    SELECT COUNT(*)
    INTO txn_count
    FROM transactions
    WHERE account_id = NEW.account_id
      AND timestamp >= NOW() - INTERVAL burst_window MINUTE;

    IF txn_count > burst_threshold THEN
        INSERT INTO fraud_alerts (txn_id, rule_id, severity)
        SELECT NEW.txn_id, rule_id, severity_level
        FROM fraud_rules
        WHERE rule_name = 'Rapid Transaction Burst'
        LIMIT 1;
    END IF;

    -- 2️⃣ High Amount Spike
    IF NEW.amount > amount_threshold THEN
        INSERT INTO fraud_alerts (txn_id, rule_id, severity)
        SELECT NEW.txn_id, rule_id, severity_level
        FROM fraud_rules
        WHERE rule_name = 'High Amount Spike'
        LIMIT 1;
    END IF;

    -- 3️⃣ Device Hopping
    SELECT COUNT(DISTINCT device_id)
    INTO device_count
    FROM transactions
    WHERE account_id = NEW.account_id
      AND timestamp >= NOW() - INTERVAL device_window MINUTE;

    IF device_count > device_threshold THEN
        INSERT INTO fraud_alerts (txn_id, rule_id, severity)
        SELECT NEW.txn_id, rule_id, severity_level
        FROM fraud_rules
        WHERE rule_name = 'Device Hopping'
        LIMIT 1;
    END IF;

END$$

DELIMITER ;

-- TESTING
INSERT INTO users (name, email) VALUES ('Test User', 'test@mail.com');

INSERT INTO accounts (user_id, account_type)
VALUES (1, 'savings');

INSERT INTO devices (device_id, device_type)
VALUES ('device_1', 'mobile'),
       ('device_2', 'laptop'),
       ('device_3', 'tablet'),
       ('device_4', 'mobile');

INSERT INTO transactions (account_id, device_id, amount)
VALUES (1, 'device_1', 200),
       (1, 'device_1', 300),
       (1, 'device_2', 150),
       (1, 'device_3', 120),
       (1, 'device_4', 100);

SELECT * FROM fraud_alerts;









-- 3.1
-- Question 1: Add a CHECK constraint to ensure transaction amount is always positive, and demonstrate it by attempting an invalid insert.
-- Add CHECK constraint to transactions table
ALTER TABLE transactions
  ADD CONSTRAINT chk_positive_amount CHECK (amount > 0);

-- Valid INSERT (succeeds)
INSERT INTO transactions (account_id, device_id, amount, transaction_type, location, ip_address)
  VALUES (1, 'device_1', 500.00, 'credit', 'Chennai', '192.168.1.1');

-- Invalid INSERT (fails with constraint violation)
INSERT INTO transactions (account_id, device_id, amount, transaction_type, location, ip_address)
  VALUES (1, 'device_1', -200.00, 'debit', 'Mumbai', '10.0.0.1');

SELECT * FROM transactions;

-- Question 2: Add a UNIQUE constraint on the email column of users and verify it prevents duplicate email registration.
-- Confirm UNIQUE constraint on email (applied at creation)
ALTER TABLE users
  ADD CONSTRAINT uq_email UNIQUE (email);

-- Try inserting a duplicate email
INSERT INTO users (name, email, phone)
  VALUES ('Eve Hackett', 'test@mail.com', '9000000001');

-- Query to verify existing unique emails
SELECT user_id, name, email FROM users;

-- Question 3: Apply a NOT NULL constraint on the severity column of fraud_alerts and query all alerts with their severity.
-- Alter column to be NOT NULL
ALTER TABLE fraud_alerts
  MODIFY COLUMN severity VARCHAR(20) NOT NULL;

-- Query all alerts with severity
SELECT alert_id, txn_id, rule_id, severity, status
  FROM fraud_alerts
  ORDER BY alert_timestamp DESC

-- 3.2 Queries Based on Aggregate Functions
-- Question 1: Find the total, average, minimum, and maximum transaction amounts per account.
SELECT a.account_id, COUNT(t.txn_id) AS total_transactions, SUM(t.amount) AS total_amount, AVG(t.amount) AS avg_amount, MIN(t.amount) AS min_amount, MAX(t.amount) AS max_amount
FROM accounts a
JOIN transactions t ON a.account_id = t.account_id
GROUP BY a.account_id
ORDER BY total_amount DESC;
ORDER BY total_amount DESC;

-- Question 2: Count the number of fraud alerts per rule and display only rules that triggered more than 1 alert (using HAVING).
SELECT
    fr.rule_id,
    fr.rule_name,
    COUNT(fa.alert_id)  AS alert_count,
    fr.severity_level
FROM fraud_rules fr
JOIN fraud_alerts fa ON fr.rule_id = fa.rule_id
GROUP BY fr.rule_id, fr.rule_name, fr.severity_level
HAVING COUNT(fa.alert_id) > 1
ORDER BY alert_count DESC;

-- Question 3: Find the overall risk score statistics across all users and identify users whose risk score is above average.
-- Step 1: Get average risk score
SELECT AVG(overall_risk_score) AS avg_risk FROM users;

-- Step 2: Find high-risk users (above average)
SELECT user_id, name, overall_risk_score
FROM users
WHERE overall_risk_score > (SELECT AVG(overall_risk_score) FROM users)
ORDER BY overall_risk_score DESC;

-- 3.3 Complex Queries Based on Sets
-- Question 1: Use UNION to get all unique account IDs that either have a high-value transaction (>10000) OR are linked to an open fraud alert.
-- Set 1: Accounts with high-value transactions
SELECT DISTINCT account_id, 'High-Value Txn' AS reason
FROM transactions
WHERE amount > 10000
UNION
-- Set 2: Accounts with open fraud alerts
SELECT DISTINCT t.account_id, 'Open Fraud Alert' AS reason
FROM transactions t
JOIN fraud_alerts fa ON t.txn_id = fa.txn_id
WHERE fa.status = 'open';

-- Question 2: Use INTERSECT (simulated with IN) to find accounts that have BOTH completed transactions AND open fraud alerts.
-- Accounts with completed transactions
SELECT DISTINCT account_id FROM transactions WHERE status = 'completed'

-- Intersect: must also have open fraud alert
SELECT DISTINCT t.account_id
FROM transactions t
WHERE t.status = 'completed'
  AND t.account_id IN (
      SELECT DISTINCT t2.account_id
      FROM transactions t2
      JOIN fraud_alerts fa ON t2.txn_id = fa.txn_id
      WHERE fa.status = 'open'
  );

-- Question 3: Use EXCEPT (simulated with NOT IN) to find accounts that have transactions but NO fraud alerts raised against them
-- Accounts with transactions but NO fraud alerts
SELECT DISTINCT account_id
FROM transactions
WHERE account_id NOT IN (
    SELECT DISTINCT t.account_id
    FROM transactions t
    JOIN fraud_alerts fa ON t.txn_id = fa.txn_id
);

-- 3.4 Complex Queries Based on Subqueries
-- Question 1: Find all transactions whose amount is greater than the average transaction amount across the entire system.
SELECT txn_id, account_id, amount, transaction_type, timestamp
FROM transactions
WHERE amount > (
    SELECT AVG(amount) FROM transactions
)
ORDER BY amount DESC;

-- Question 2: Find users who own accounts that have been involved in at least one HIGH severity fraud alert.
SELECT DISTINCT u.user_id, u.name, u.email, u.overall_risk_score
FROM users u
WHERE u.user_id IN (
    SELECT a.user_id
    FROM accounts a
    WHERE a.account_id IN (
        SELECT t.account_id
        FROM transactions t
        WHERE t.txn_id IN (
            SELECT fa.txn_id
            FROM fraud_alerts fa
            WHERE fa.severity = 'high' AND fa.status = 'open'
        )
    )
);

-- Question 3: Retrieve fraud rules that have NEVER triggered any alert (using NOT EXISTS).
SELECT rule_id, rule_name, severity_level, is_active
FROM fraud_rules fr
WHERE NOT EXISTS (
    SELECT 1 FROM fraud_alerts fa
    WHERE fa.rule_id = fr.rule_id
)
ORDER BY rule_id;

-- 3.5 Complex Queries Based on Joins
-- Question 1: Use INNER JOIN to retrieve all fraud alerts along with transaction details, user name, and rule name.
SELECT
    fa.alert_id,
    u.name        AS customer_name,
    t.amount,
    t.location,
    fr.rule_name,
    fa.severity,
    fa.status,
    fa.alert_timestamp
FROM fraud_alerts fa
INNER JOIN transactions t  ON fa.txn_id  = t.txn_id
INNER JOIN accounts a      ON t.account_id = a.account_id
INNER JOIN users u         ON a.user_id  = u.user_id
INNER JOIN fraud_rules fr  ON fa.rule_id  = fr.rule_id
ORDER BY fa.alert_timestamp DESC;

-- Question 2: Use LEFT JOIN to find all users and their associated fraud alerts (including users with no alerts).
SELECT
    u.user_id,
    u.name,
    COUNT(fa.alert_id)  AS total_alerts,
    u.overall_risk_score
FROM users u
LEFT JOIN accounts a   ON u.user_id    = a.user_id
LEFT JOIN transactions t ON a.account_id = t.account_id
LEFT JOIN fraud_alerts fa ON t.txn_id   = fa.txn_id
GROUP BY u.user_id, u.name, u.overall_risk_score
ORDER BY total_alerts DESC;

-- Question 3: Self-join on transactions to find pairs of transactions from the same account within a 10-minute window (rapid transaction detection).
SELECT
    t1.txn_id        AS txn1_id,
    t2.txn_id        AS txn2_id,
    t1.account_id,
    t1.amount        AS amt1,
    t2.amount        AS amt2,
    TIMESTAMPDIFF(MINUTE, t1.timestamp, t2.timestamp) AS gap_minutes
FROM transactions t1
JOIN transactions t2
    ON  t1.account_id = t2.account_id
    AND t2.txn_id > t1.txn_id
    AND TIMESTAMPDIFF(MINUTE, t1.timestamp, t2.timestamp) <= 10
ORDER BY t1.account_id, gap_minutes;

-- 3.6 Complex Queries Based on Views
-- Question 1: Create a view to show all open high-severity fraud alerts with full transaction and user details for a monitoring dashboard.
CREATE VIEW vw_high_severity_alerts AS
SELECT
    fa.alert_id,
    u.user_id,
    u.name          AS customer_name,
    a.account_id,
    t.txn_id,
    t.amount,
    t.location,
    fr.rule_name,
    fa.severity,
    fa.alert_timestamp
FROM fraud_alerts fa
JOIN transactions t  ON fa.txn_id   = t.txn_id
JOIN accounts a      ON t.account_id = a.account_id
JOIN users u         ON a.user_id    = u.user_id
JOIN fraud_rules fr  ON fa.rule_id   = fr.rule_id
WHERE fa.severity = 'high' AND fa.status = 'open';

-- Query the view
SELECT * FROM vw_high_severity_alerts;

-- Question 2: Create a view that summarizes per-account transaction statistics and use it to find accounts with suspiciously high average transaction amounts.
CREATE VIEW vw_account_txn_summary AS
SELECT
    a.account_id,
    u.name          AS owner,
    COUNT(t.txn_id) AS txn_count,
    SUM(t.amount)   AS total_amount,
    AVG(t.amount)   AS avg_amount,
    MAX(t.amount)   AS max_amount
FROM accounts a
JOIN users u        ON a.user_id    = u.user_id
JOIN transactions t ON a.account_id = t.account_id
GROUP BY a.account_id, u.name;

-- Use the view to identify high-risk accounts
SELECT * FROM vw_account_txn_summary
WHERE avg_amount > 5000
ORDER BY avg_amount DESC;

-- Question 3: Create a view to list all fraud rules that are currently active along with how many times they have been triggered.
CREATE VIEW vw_active_rule_performance AS
SELECT
    fr.rule_id,
    fr.rule_name,
    fr.severity_level,
    fr.threshold_value,
    fr.time_window_minutes,
    COUNT(fa.alert_id) AS times_triggered
FROM fraud_rules fr
LEFT JOIN fraud_alerts fa ON fr.rule_id = fa.rule_id
WHERE fr.is_active = TRUE
GROUP BY fr.rule_id, fr.rule_name, fr.severity_level,
         fr.threshold_value, fr.time_window_minutes
ORDER BY times_triggered DESC;

SELECT * FROM vw_active_rule_performance;

-- 3.7 Complex Queries Based on Triggers
-- Question 1: Create a trigger that automatically inserts a fraud alert when a transaction amount exceeds 10,000 (High Amount Rule).
DELIMITER $$
CREATE TRIGGER trg_high_amount_alert
AFTER INSERT ON transactions
FOR EACH ROW
BEGIN
    IF NEW.amount > 10000 THEN
        INSERT INTO fraud_alerts (txn_id, rule_id, severity, status)
        VALUES (NEW.txn_id, 2, 'high', 'open');
    END IF;
END$$
DELIMITER ;

-- Test the trigger
INSERT INTO transactions (account_id, device_id, amount, transaction_type, location)
  VALUES (1, 'device_1', 12000.00, 'debit', 'Hyderabad');

-- Verify alert was auto-created
SELECT * FROM fraud_alerts ORDER BY alert_id DESC LIMIT 1;

-- Question 2: Create a trigger that updates the user's overall_risk_score by +5 every time a high-severity alert is raised on their account.
DELIMITER $$
CREATE TRIGGER trg_update_risk_score
AFTER INSERT ON fraud_alerts
FOR EACH ROW
BEGIN
    DECLARE v_user_id INT;
    IF NEW.severity = 'high' THEN
        SELECT a.user_id INTO v_user_id
        FROM transactions t
        JOIN accounts a ON t.account_id = a.account_id
        WHERE t.txn_id = NEW.txn_id
        LIMIT 1;
        UPDATE users
        SET overall_risk_score = overall_risk_score + 5
        WHERE user_id = v_user_id;
    END IF;
END$$
DELIMITER ;

-- Check risk score before and after a high alert insert
SELECT user_id, name, overall_risk_score FROM users WHERE user_id = 1;

-- Question 3: Create a BEFORE DELETE trigger to prevent deletion of fraud alerts that are still in 'open' status.
DELIMITER $$
CREATE TRIGGER trg_prevent_open_alert_delete
BEFORE DELETE ON fraud_alerts
FOR EACH ROW
BEGIN
    IF OLD.status = 'open' THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Cannot delete an open fraud alert. Resolve it first.';
    END IF;
END$$
DELIMITER ;

-- Attempt to delete an open alert
DELETE FROM fraud_alerts WHERE alert_id = 1;

-- Close alert first, then delete (succeeds)
UPDATE fraud_alerts SET status = 'resolved' WHERE alert_id = 1;
DELETE FROM fraud_alerts WHERE alert_id = 1;

-- 3.8 Complex Queries Based on Cursors
-- Question 1: Write a stored procedure using a cursor to iterate over all open fraud alerts and print a summary message for each.
DELIMITER $$
CREATE PROCEDURE proc_summarize_open_alerts()
BEGIN
    DECLARE v_alert_id   INT;
    DECLARE v_txn_id     INT;
    DECLARE v_severity   VARCHAR(20);
    DECLARE v_done       INT DEFAULT 0;

    DECLARE cur_alerts CURSOR FOR
        SELECT alert_id, txn_id, severity
        FROM fraud_alerts
        WHERE status = 'open';

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

    CREATE TEMPORARY TABLE IF NOT EXISTS alert_log(
        log_msg TEXT
    );

    OPEN cur_alerts;
    read_loop: LOOP
        FETCH cur_alerts INTO v_alert_id, v_txn_id, v_severity;
        IF v_done = 1 THEN LEAVE read_loop; END IF;
        INSERT INTO alert_log (log_msg)
        VALUES (CONCAT('[ALERT ', v_alert_id, '] TXN: ', v_txn_id,
                       ' | Severity: ', UPPER(v_severity)));
    END LOOP;
    CLOSE cur_alerts;

    SELECT * FROM alert_log;
    DROP TEMPORARY TABLE IF EXISTS alert_log;
END$$
DELIMITER ;

CALL proc_summarize_open_alerts();

-- Question 2: Use a cursor to auto-update the risk score of each user by accumulating severity weights across all their associated alerts.
DELIMITER $$
CREATE PROCEDURE proc_recalculate_risk_scores()
BEGIN
    DECLARE v_user_id    INT;
    DECLARE v_score      DECIMAL(10,2);
    DECLARE v_done       INT DEFAULT 0;

    DECLARE cur_users CURSOR FOR
        SELECT u.user_id,
               IFNULL(SUM(CASE fa.severity
                           WHEN 'high'   THEN 10
                           WHEN 'medium' THEN 5
                           ELSE 1 END), 0) AS risk
        FROM users u
        LEFT JOIN accounts a   ON u.user_id = a.user_id
        LEFT JOIN transactions t ON a.account_id = t.account_id
        LEFT JOIN fraud_alerts fa ON t.txn_id = fa.txn_id
        GROUP BY u.user_id;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

    OPEN cur_users;
    score_loop: LOOP
        FETCH cur_users INTO v_user_id, v_score;
        IF v_done = 1 THEN LEAVE score_loop; END IF;
        UPDATE users SET overall_risk_score = v_score
        WHERE user_id = v_user_id;
    END LOOP;
    CLOSE cur_users;

    SELECT user_id, name, overall_risk_score FROM users;
END$$
DELIMITER ;

CALL proc_recalculate_risk_scores();

-- Question 3: Write a cursor-based procedure that flags and deactivates fraud rules which have NOT been triggered in the past 30 days.
DELIMITER $$
CREATE PROCEDURE proc_deactivate_stale_rules()
BEGIN
    DECLARE v_rule_id     INT;
    DECLARE v_rule_name   VARCHAR(100);
    DECLARE v_last_alert  TIMESTAMP;
    DECLARE v_done        INT DEFAULT 0;

    DECLARE cur_rules CURSOR FOR
        SELECT fr.rule_id, fr.rule_name,
               MAX(fa.alert_timestamp) AS last_alert
        FROM fraud_rules fr
        LEFT JOIN fraud_alerts fa ON fr.rule_id = fa.rule_id
        WHERE fr.is_active = TRUE
        GROUP BY fr.rule_id, fr.rule_name
        HAVING last_alert IS NULL
            OR MAX(fa.alert_timestamp) < NOW() - INTERVAL 30 DAY;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

    OPEN cur_rules;
    deact_loop: LOOP
        FETCH cur_rules INTO v_rule_id, v_rule_name, v_last_alert;
        IF v_done = 1 THEN LEAVE deact_loop; END IF;
        UPDATE fraud_rules SET is_active = FALSE
        WHERE rule_id = v_rule_id;
    END LOOP;
    CLOSE cur_rules;

    SELECT rule_id, rule_name, is_active FROM fraud_rules;
END$$
DELIMITER ;

CALL proc_deactivate_stale_rules();
