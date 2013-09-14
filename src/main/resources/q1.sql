SELECT
  sum(COLUMN1) as C1,
  round(( sum(COLUMN7) )/decode(( sum(COLUMN1) ),0,1,( sum(COLUMN1) )),4),
  round((( sum(COLUMN8) )/ decode(( sum(COLUMN1) ),0,1,( sum(COLUMN1) )) ) * 100 ,4),
  COLUMN4,
  COLUMN5,
  COLUMN6,
  avg(( COLUMN2 )),
  avg(( COLUMN3 )) FROM
  TABLE1
WHERE
  DT  =  '1373221800000'
GROUP BY
  COLUMN4,
  COLUMN5,
  COLUMN6