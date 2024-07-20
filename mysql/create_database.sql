-- Create database Historical_Data if it doesn't exist
CREATE DATABASE IF NOT EXISTS Historical_Data;

-- Switch to the created database
USE Historical_Data;

-- Create table DT_EXCHANGES
CREATE TABLE IF NOT EXISTS DT_EXCHANGES (
    id_exchange INT AUTO_INCREMENT PRIMARY KEY,
    exchange_1 VARCHAR(50) NOT NULL,
    exchange_2 VARCHAR(50) NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    creation_date VARCHAR(50) NOT NULL
);

-- Create table DT_TIME
CREATE TABLE IF NOT EXISTS DT_TIME (
    id_date VARCHAR(8) NOT NULL,
    Date DATE NOT NULL,
    CalendarYear VARCHAR(4) NOT NULL,
    CalendarYearMonth VARCHAR(6) NOT NULL,
    CalendarYearWeek VARCHAR(6) NOT NULL,
    DayWeekOfYear INT NOT NULL,
    MonthOfYear INT NOT NULL,
    PRIMARY KEY (id_date)
);

-- Create table FT_DAILY_DATA
CREATE TABLE IF NOT EXISTS FT_DAILY_DATA (
    Open DECIMAL(18, 0) NOT NULL,
    High DECIMAL(18, 0) NOT NULL,
    Low DECIMAL(18, 0) NOT NULL,
    Close DECIMAL(18, 0) NOT NULL,
    adj_close DECIMAL(18, 0) NOT NULL,
    Volume DECIMAL(18, 0) NOT NULL,
    Exchange VARCHAR(50) NOT NULL,
    id_exchange INT NOT NULL,
    id_date VARCHAR(8) NOT NULL,
    CONSTRAINT FK_FT_DAILY_DATA_DT_EXCHANGES FOREIGN KEY (id_exchange) REFERENCES DT_EXCHANGES (id_exchange),
    CONSTRAINT FK_FT_DAILY_DATA_DT_TIME FOREIGN KEY (id_date) REFERENCES DT_TIME (id_date)
);



-- Create table FT_HOUR_DATA
CREATE TABLE IF NOT EXISTS FT_HOUR_DATA (
    Open DECIMAL(18, 0) NOT NULL,
    High DECIMAL(18, 0) NOT NULL,
    Low DECIMAL(18, 0) NOT NULL,
    Close DECIMAL(18, 0) NOT NULL,
    adj_close DECIMAL(18, 0) NOT NULL,
    Volume DECIMAL(18, 0) NOT NULL,
    Hour INT NOT NULL,
    Exchange VARCHAR(50) NOT NULL,
    id_exchange INT NOT NULL,
    id_date VARCHAR(8) NOT NULL,
    CONSTRAINT FK_FT_HOUR_EXCHANGE FOREIGN KEY (id_exchange) REFERENCES DT_EXCHANGES (id_exchange),
    CONSTRAINT FK_FT_HOUR_DT_TIME FOREIGN KEY (id_date) REFERENCES DT_TIME (id_date)
);


-- Create table FT_HOUR_DATA
CREATE TABLE IF NOT EXISTS FT_FEAR_DATA (
    Value INT NOT NULL,
    value_classification VARCHAR(50) NOT NULL,
    id_date VARCHAR(8) NOT NULL
);






