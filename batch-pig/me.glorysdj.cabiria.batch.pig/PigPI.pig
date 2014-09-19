REGISTER udf-pig-0.0.1-SNAPSHOT.jar;

define Pow me.glorysdj.cabiria.udf.pig.math.Pow();

A = LOAD '/user/root/input/pi' USING PigStorage() AS (num:double);
B = FOREACH A GENERATE Pow(-1.0,$0)*(1.0/($0*2.0-1.0));
C = GROUP B ALL;
D = FOREACH C GENERATE 4.0*SUM(B.$0);

STORE D INTO 'output/pi5' USING PigStorage();

--  pig -x mapreduce PigPI.pig