WORDS = LOAD 'parsedsyslog' AS (rawword: chararray, position: int, node: chararray, hour: int);
C = FOREACH WORDS GENERATE node, 1 as partial;
D = DISTINCT C;
E = GROUP D BY partial;
F = FOREACH E GENERATE COUNT(D);
STORE F INTO 'numofnodes.dat' USING PigStorage();
