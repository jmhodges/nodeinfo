REGISTER nodeinfo.jar
WORDS = LOAD 'parsedsyslog' AS (rawword: chararray, position: int, node: chararray, hour: int);

YGROUP = GROUP WORDS BY (rawword, position, node, hour);
Y = FOREACH YGROUP GENERATE group, COUNT(WORDS) as wordpernodeperhourcount;

XGROUP = GROUP Y BY (group.rawword, group.position, group.node);
X = FOREACH XGROUP GENERATE group, SUM(Y.wordpernodeperhourcount) as wordpernodecount;

XWITHLOG = FOREACH X GENERATE group, wordpernodecount, (wordpernodecount*nodeinfo.LOG2(wordpernodecount)) as weirdwordpernode;

WORDGROUP = GROUP XWITHLOG BY (group.rawword, group.position);
DANDE = FOREACH WORDGROUP GENERATE group, SUM(XWITHLOG.wordpernodecount) as dw, SUM(XWITHLOG.weirdwordpernode) as ew;

ALLGW = FOREACH DANDE GENERATE group, (1.0 + (1.0 / nodeinfo.LOG2($numofnodes)) * (ew / dw - nodeinfo.LOG2(dw))) as gw;
GW = FILTER ALLGW BY gw > 0.0;

YANDG = JOIN GW BY (group.rawword, group.position), Y BY (group.rawword, group.position);
PARTIAL = FOREACH YANDG GENERATE Y::group.rawword as rawword, Y::group.position as position, Y::group.node as node, Y::group.hour as hour, (GW::gw * nodeinfo.LOG2(Y::wordpernodeperhourcount)) as partial;

FULLGROUP = GROUP PARTIAL BY (node, hour);
NODEINFO = FOREACH FULLGROUP GENERATE group.node, group.hour, SUM(PARTIAL.partial) as oddness;

SORTEDNODEINFO = ORDER NODEINFO BY oddness DESC;

DUMP SORTEDNODEINFO;
