explain analyze SELECT
100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end)
/ sum(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM
lineitem
JOIN
part
ON
l_partkey = p_partkey
WHERE
l_shipdate >= date '1993-10-01'
AND l_shipdate < date '1993-10-01' + interval '1' month;
