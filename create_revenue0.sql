CREATE OR REPLACE VIEW revenue0 AS
SELECT
    l_suppkey AS supplier_no,
    SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-08-01'
  AND l_shipdate < DATE '1994-08-01' + INTERVAL '3' MONTH
GROUP BY l_suppkey;

