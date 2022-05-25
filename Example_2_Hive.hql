-- 

CREATE TABLE `salary`(
  `emp_no` string,
  `salary` bigint,
  `from_date` string,
  `end_date` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 'file:/Users/staticor/tmp/hive/warehouse/shopee.db/salary'
TBLPROPERTIES ("skip.header.line.count"="1")
  ;

hive> load data inpath '/Users/staticor/hive/WindowQueryInSparkHive/data/salaries.csv'  overwrite  into table  salary;
load data inpath '/Users/staticor/hive/WindowQueryInSparkHive/data/salaries.csv'  overwrite into table  salary;



-- 样例SQL
-- 查询 empno 某个人， 
SELECT emp_no, 
       from_date,
       salary,
       prev_salary, 
       ROUND(((1 - prev_salary/salary) * 100),2) AS change
FROM  (
       SELECT emp_no, 
              from_date, 
              salary, 
              LAG(salary,1,salary) OVER (PARTITION BY emp_no ORDER BY from_date) AS prev_salary
       FROM salary ) a 
WHERE emp_no = 10001;


--   row_number 函数
-- 

SELECT emp_no, 
       salary, 
       from_date, 
       end_date, 
       r
FROM   ( SELECT emp_no, 
                salary, 
                from_date, 
                end_date, 
                rank() OVER ( ORDER BY salary DESC) AS r 
         FROM   salary
         WHERE  from_date <= current_date() AND 
                end_date >=  current_date() ) a
WHERE r in (1,2);