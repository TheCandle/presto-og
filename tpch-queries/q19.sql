explain analyze 
select l_extendedprice, l_discount
FROM
lineitem
WHERE
l_shipdate >= date '1993-10-01'
AND l_shipdate < date '1993-10-01' + interval '1' month;
