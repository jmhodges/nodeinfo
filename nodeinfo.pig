REGISTER nodeinfo.jar

LINE = LOAD 'smallsystem.log' USING TextLoader();
WORD = FOREACH LINE GENERATE FLATTEN(nodeinfo.WordedSyslog($0));

YGROUP = GROUP WORD BY (word, position, node, hour);
Y = FOREACH YGROUP GENERATE group, COUNT(WORD) as wordpernodeperhourcount;

XGROUP = GROUP Y BY (group.word, group.position, group.node);
X = FOREACH XGROUP GENERATE group, SUM(Y.wordpernodeperhourcount) as wordpernodecount;

XWITHLOG = FOREACH X GENERATE group, wordpernodecount, (wordpernodecount*nodeinfo.LOG2(wordpernodecount)) as weirdwordpernode;

WORDGROUP = GROUP XWITHLOG BY (group.word, group.position);
DANDE = FOREACH WORDGROUP GENERATE group, SUM(XWITHLOG.wordpernodecount) as dw, SUM(XWITHLOG.weirdwordpernode) as ew;

ALLGW = FOREACH DANDE GENERATE group, (1.0 + (1.0 / nodeinfo.LOG2($numofnodes)) * (ew / dw - nodeinfo.LOG2(dw))) as gw;
GW = FILTER ALLGW BY gw > 0.0;

YANDG = JOIN GW BY (group.word, group.position), Y BY (group.word, group.position);

PARTIAL = FOREACH YANDG GENERATE Y::group.word as word, Y::group.position as position, Y::group.node as node, Y::group.hour as hour, (GW::gw * nodeinfo.LOG2(Y::wordpernodeperhourcount)) as partial;

-- wtf how is one supposed to use piggybank's POW? arghhhhh.
PARTIAL2 = FOREACH PARTIAL GENERATE word, position, node, hour, partial*partial as partial;

FULLGROUP = GROUP PARTIAL2 BY (node, hour);
NODEINFO = FOREACH FULLGROUP GENERATE group.node, group.hour, SUM(PARTIAL2.partial) as oddness;

SORTEDNODEINFO = ORDER NODEINFO BY oddness DESC;

DUMP SORTEDNODEINFO;
