SELECT
  sum(COLUMN1) as C1,
  round(( sum(COLUMN7) )/decode(( sum(COLUMN1) ),0,1,( sum(COLUMN1) )),4),
  round((( sum(COLUMN8) )/ decode(( sum(COLUMN1) ),0,1,( sum(COLUMN1) )) ) * 100 ,4),
  sum(COLUMN8) as C4,
  COLUMN4,
  COLUMN5,
  COLUMN6
FROM
  TABLE1
WHERE
  DT  =  '1373221800000'
GROUP BY
  COLUMN4,
  COLUMN5,
  COLUMN6
HAVING
  sum(COLUMN1)  <>  0
