-- REPORT 1: Sales Summary per Periode, Class, Model

CREATE TABLE IF NOT EXISTS dm_sales_summary (
    periode      VARCHAR(7)    NOT NULL COMMENT 'Format YYYY-MM',
    class        VARCHAR(10)   NOT NULL COMMENT 'LOW / MEDIUM / HIGH',
    model        VARCHAR(50)   NOT NULL,
    total        BIGINT        NOT NULL COMMENT 'Total nominal penjualan',
    updated_at   DATETIME      NOT NULL,
    PRIMARY KEY (periode, class, model)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Truncate + insert ulang setiap daily run
TRUNCATE TABLE dm_sales_summary;

INSERT INTO dm_sales_summary (periode, class, model, total, updated_at)
SELECT
    DATE_FORMAT(s.invoice_date, '%Y-%m')          AS periode,
    CASE
        WHEN s.price_clean BETWEEN 100000000 AND 250000000 THEN 'LOW'
        WHEN s.price_clean BETWEEN 250000001 AND 400000000 THEN 'MEDIUM'
        WHEN s.price_clean > 400000000             THEN 'HIGH'
        ELSE 'UNKNOWN'
    END                                            AS class,
    s.model,
    SUM(s.price_clean)                             AS total,
    NOW()                                          AS updated_at
FROM sales_clean s
WHERE s.is_duplicate = FALSE        -- buang duplikat
  AND s.price_clean IS NOT NULL
GROUP BY
    DATE_FORMAT(s.invoice_date, '%Y-%m'),
    class,
    s.model
ORDER BY periode, class, model;


-- REPORT 2: After-Sales Customer Summary per Tahun


CREATE TABLE IF NOT EXISTS dm_aftersales_summary (
    periode         VARCHAR(4)    NOT NULL COMMENT 'Format YYYY',
    vin             VARCHAR(20)   NOT NULL,
    customer_name   VARCHAR(100),
    address         TEXT,
    count_service   INT           NOT NULL,
    priority        VARCHAR(10)   NOT NULL COMMENT 'HIGH / MED / LOW',
    updated_at      DATETIME      NOT NULL,
    PRIMARY KEY (periode, vin)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

TRUNCATE TABLE dm_aftersales_summary;

INSERT INTO dm_aftersales_summary
    (periode, vin, customer_name, address, count_service, priority, updated_at)
SELECT
    YEAR(a.service_date)                           AS periode,
    a.vin,
    c.name                                         AS customer_name,
    -- Gabung address (bisa NULL kalau tidak ada di customer_addresses)
    CONCAT_WS(', ',
        NULLIF(TRIM(ca.address), ''),
        NULLIF(TRIM(ca.city), ''),
        NULLIF(TRIM(ca.province), '')
    )                                              AS address,
    COUNT(a.service_ticket)                        AS count_service,
    CASE
        WHEN COUNT(a.service_ticket) > 10  THEN 'HIGH'
        WHEN COUNT(a.service_ticket) >= 5  THEN 'MED'
        ELSE                                    'LOW'
    END                                            AS priority,
    NOW()                                          AS updated_at
FROM after_sales_clean      a
JOIN customers_clean        c   ON a.customer_id = c.id
LEFT JOIN customer_addresses ca ON a.customer_id = ca.customer_id
WHERE a.vin_in_sales = TRUE          -- hanya VIN yang dikenal
GROUP BY
    YEAR(a.service_date),
    a.vin,
    c.name,
    ca.address,
    ca.city,
    ca.province
ORDER BY periode DESC, count_service DESC;
