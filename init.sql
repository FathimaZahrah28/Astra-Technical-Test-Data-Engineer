
-- init.sql – Setup database Maju Jaya (raw tables + sample data)


CREATE DATABASE IF NOT EXISTS maju_jaya CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE maju_jaya;

--  customers_raw 
DROP TABLE IF EXISTS customers_raw;
CREATE TABLE customers_raw (
    id          INT PRIMARY KEY,
    name        VARCHAR(100),
    dob         VARCHAR(20),    -- sengaja VARCHAR karena format belum clean
    created_at  DATETIME(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO customers_raw VALUES
(1, 'Antonio',       '1998-08-04',  '2025-03-01 14:24:40.012'),
(2, 'Brandon',       '2001-04-21',  '2025-03-02 08:12:54.003'),
(3, 'Charlie',       '1980/11/15',  '2025-03-02 11:20:02.391'),
(4, 'Dominikus',     '14/01/1995',  '2025-03-03 09:50:41.852'),
(5, 'Erik',          '1900-01-01',  '2025-03-03 17:22:03.198'),
(6, 'PT Black Bird', NULL,          '2025-03-04 12:52:16.122');

-- sales_raw
DROP TABLE IF EXISTS sales_raw;
CREATE TABLE sales_raw (
    vin          VARCHAR(20),
    customer_id  INT,
    model        VARCHAR(50),
    invoice_date DATE,
    price        VARCHAR(20),    -- sengaja VARCHAR karena pakai titik ribuan
    created_at   DATETIME(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO sales_raw VALUES
('JIS8135SAD', 1, 'RAIZA',  '2025-03-01', '350.000.000', '2025-03-01 14:24:40.012'),
('MAS8160POE', 3, 'RANGGO', '2025-05-19', '430.000.000', '2025-05-19 14:29:21.003'),
('JLK1368KDE', 4, 'INNAVO', '2025-05-22', '600.000.000', '2025-05-22 16:10:28.120'),
('JLK1869KDF', 6, 'VELOS',  '2025-08-02', '390.000.000', '2025-08-02 14:04:31.021'),
('JLK1962KOP', 6, 'VELOS',  '2025-08-02', '390.000.000', '2025-08-02 15:21:04.201');  -- suspect duplicate

-- after_sales_raw 
DROP TABLE IF EXISTS after_sales_raw;
CREATE TABLE after_sales_raw (
    service_ticket  VARCHAR(20),
    vin             VARCHAR(20),
    customer_id     INT,
    model           VARCHAR(50),
    service_date    DATE,
    service_type    VARCHAR(10),
    created_at      DATETIME(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO after_sales_raw VALUES
('T124-kgu1', 'MAS8160POE', 3, 'RANGGO', '2025-07-11', 'BP', '2025-07-11 09:24:40.012'),
('T560-jga1', 'JLK1368KDE', 4, 'INNAVO', '2025-08-04', 'PM', '2025-08-04 10:12:54.003'),
('T521-oai8', 'POI1059IIK', 5, 'RAIZA',  '2026-09-10', 'GR', '2026-09-10 12:45:02.391');  -- VIN tidak di sales_raw

-- customer_addresses
DROP TABLE IF EXISTS customer_addresses;
CREATE TABLE customer_addresses (
    id           INT AUTO_INCREMENT PRIMARY KEY,
    customer_id  INT,
    address      TEXT,
    city         VARCHAR(100),
    province     VARCHAR(100),
    created_at   DATETIME(3),
    ingested_at  DATETIME,
    UNIQUE KEY uq_customer (customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- (diisi oleh pipeline Task 1, tapi seed sample untuk testing)
INSERT INTO customer_addresses (customer_id, address, city, province, created_at) VALUES
(1, 'Jalan Mawar V, RT 1/RW 2', 'Bekasi',          'Jawa Barat',  '2026-03-01 14:24:40.012'),
(3, 'Jl Ababil Indah',          'Tangerang Selatan','Jawa Barat',  '2026-03-01 14:24:40.012'),
(4, 'Jl. Kemang Raya 1 No 3',  'Jakarta Pusat',    'DKI Jakarta', '2026-03-01 14:24:40.012'),
(6, 'Astra Tower, Jalan Yos Sudarso 12', 'Jakarta Utara', 'DKI Jakarta', '2026-03-01 14:24:40.012');
