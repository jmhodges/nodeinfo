REGISTER nodeinfo.jar

LINE = LOAD 'smallsystem.log' USING TextLoader();
WORD = FOREACH LINE GENERATE FLATTEN(nodeinfo.WordedSyslog($0));
C = FOREACH WORD GENERATE node, 1 as partial;
D = DISTINCT C;
E = GROUP D BY partial;
F = FOREACH E GENERATE COUNT(D);
STORE F INTO 'numofnodes.dat' USING PigStorage();
