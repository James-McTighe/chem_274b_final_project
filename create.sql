CREATE TABLE IF NOT EXISTS user_data(
    account_id varchar(255) UNIQUE,
    account_time_stamp TIMESTAMP,
    active BOOLEAN DEFAULT True,
    merge_id VARCHAR(255),
    merge_date TIMESTAMP,
    account_balance INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS balances(
    account_id varchar(255),
    amount INT,
    account_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions(
    accout_id VARCHAR(255),
    amount INT,
    date_of_transaction TIMESTAMP,
    type_of_transaction VARCHAR(255),
    cashback_date TIMESTAMP
);

