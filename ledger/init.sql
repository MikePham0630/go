CREATE USER 'ledger_user'@'%' IDENTIFIED BY 'Auth123';

CREATE DATABASE IF NOT EXISTS ledger;

GRANT ALL PRIVILEGES ON ledger.* TO 'ledger_user'@'%';

USE ledger;

CREATE TABLE `ledger` (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    amount INT NOT NULL,
    operation VARCHAR(250) NOT NULL,
    transaction_date VARCHAR(250) NOT NULL
);