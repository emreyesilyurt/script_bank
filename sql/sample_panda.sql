-- sample_panda.sql
SELECT * FROM `datadojo.prod.panda`
WHERE RAND() < 0.00001
LIMIT 10000;
