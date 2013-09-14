create table PS_Col51_1_DAY (
Table31_col1 string,
Table51_col2 string,
col1 int,
col2 int,
col3 int,
col4 int,
col5 int,
col6 int,
col7 int,
col8 int,
col9 int,
col10 int,
col11 int,
col12 int,
col13 int,
col14 int,
col15 int,
col16 int,
col17 int,
col18 int,
Col51_1_2 string,
Col51_1_3 string,
test_Col51 int,
CELL_SAC string
)
partitioned by (DT string)  --1374345000000;

create table table51 (
Table51_col1 string,
Table51_col3 string,
Col51_1_1 string
);

set mapred.reduce.tasks.speculative.execution=false;
set mapred.reduce.tasks=20;
SELECT
  COUNT(PS_Col51_1_DAY.Table31_col1),
  ROUND(avg(PS_Col51_1_DAY.Col1),2),
  ROUND(avg(PS_Col51_1_DAY.col2),2),
  ROUND(AVG(PS_Col51_1_DAY.col3),2),
  ROUND(avg(PS_Col51_1_DAY.col4),2),
  ROUND(Avg(PS_Col51_1_DAY.col5),2),
  ROUND(AVG(PS_Col51_1_DAY.col6),2),
  ROUND(AVG(PS_Col51_1_DAY.col7),2),
  ROUND(AVG(PS_Col51_1_DAY.col8),2),
  ROUND(AVG(PS_Col51_1_DAY.col9),2),
  ROUND(AVG(PS_Col51_1_DAY.col10),2),
  ROUND(AVG(PS_Col51_1_DAY.col11),2),
  ROUND(AVG(PS_Col51_1_DAY.col12),2),
  ROUND(AVG(PS_Col51_1_DAY.col13),2),
  ROUND(AVG(PS_Col51_1_DAY.col14),2),
  ROUND(AVG(PS_Col51_1_DAY.col15),2),
  ROUND(AVG(PS_Col51_1_DAY.col16),2),
  ROUND(AVG(PS_Col51_1_DAY.col17),2),
  ROUND(AVG(PS_Col51_1_DAY.col18),2),
  Table51.Col51_1_1,
  PS_Col51_1_DAY.Col51_1_2,
  PS_Col51_1_DAY.Col51_1_3, 
  ROUND(AVG(PS_Col51_1_DAY.test_Col51),0) as C23 
FROM
  PS_Col51_1_DAY RIGHT OUTER JOIN Table51 ON (
    PS_Col51_1_DAY.Table51_col2=Table51.Table51_col1
    AND
    PS_Col51_1_DAY.CELL_SAC=Table51.Table51_col3
  )
WHERE
  (
   PS_Col51_1_DAY.test_Col51 Is Not Null 
   AND
   PS_Col51_1_DAY.DT = '1374345000000'
  )
GROUP BY
  Table51.Col51_1_1,
  PS_Col51_1_DAY.Col51_1_2,
  PS_Col51_1_DAY.Col51_1_3
ORDER BY C23;
