
SELECT LAG(date1,1) OVER (ORDER BY date1) AS previous, 
       date1, 
       LEAD(date1,1) OVER (ORDER BY date1) AS next 
FROM foo;
-----
Lag 函数与Lead函数示例

----

-- Just show first 10 rows
SELECT LAG(bizdate,1) OVER (ORDER BY bizdate) AS previous,   
       bizdate, 
       LEAD(bizdate,1) OVER (ORDER BY bizdate) AS next 
FROM foo limit 10 
;
