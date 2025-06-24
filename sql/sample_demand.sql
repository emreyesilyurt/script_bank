-- sample_demand.sql
SELECT * FROM `datadojo.prod.demand_normalized`
WHERE RAND() < 0.00001
LIMIT 10000;
