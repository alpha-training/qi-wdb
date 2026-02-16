/ adapted from https://github.com/simongarland/tick/blob/master/w.q
.qi.import`event
.qi.import`cron

KOE:any`keeponexit`koe in key .qi.opts
gettmppath:{hsym`$$[`tmpPath in key .conf;.conf.tmpPath;.conf.DATA,"/tmp"],"/wdb.",string[.z.i],".",string x}
TMPPATH:gettmppath .z.d
maketmp:{.[` sv TMPPATH,x,`;();,;.Q.en[TMPPATH]`. x]} / have a updtmp and clear function
clear:{@[`.;tables`;0#]}
writeandclear:{maketmp each t:tables`;clear`}
writeall:{-1"moving tables out of memory and onto disk at: ",(8#2_string .z.n)," UKT";writeandclear`}
memcheck:{if[.conf.WDB_MAXMB<first system["w"]%1024*1024;writeandclear`]}

append:{[t;data]
    t insert data;
     if[.conf.MAXROWS<count get t;writeandclear`]
 }
upd:append

disksort:{[t;c;a]
    if[not`s~attr(t:hsym t)c; / if its already sorted we skip everything (no need to sort a sorted list)
        if[count t; / if the table is empty, there is nothing to sort
            ii:iasc iasc flip c!t c,:(); / this tells you the index each number needs to go in order for the list to be sortedi
            if[not$[(0,-1+count ii)~(first;last)@\:ii;@[{`s#x;1b};ii;0b];0b]; / if the first and last indices are 0&N-1. then it might be sorted. try to apply the sorted attribute 
               {v:get y;if[not$[all(fv:first v)~/:256#v;all fv~/:v;0b];v[x]:v;y set v];}[ii]each` sv't,'get` sv t,`.d / on each column file within each tmp
              ]
          ];
        @[t;first c;a] / apply the parted attribute on each sym col
      ];t}

.u.end:{ / end of day: save, clear, sort on disk, move, hdb reload
    t:tables`.;t@:where 11h=type each t@\:`sym;
    maketmp each t;
    @[`.;t;0#];
    {disksort[` sv TMPPATH,x,`;`sym;`p#]}each t; /sort on disk by sym and set `p#
    system"mv ",(1_string TMPPATH)," ",.conf.DATA,"/HDB/",string .z.d; / can change this for par.txt -1_1_string .Q.par[`:.;x;`];
    TMPPATH::gettmppath .z.d;
    .Q.gc`;	
    $[null h:.ipc.conn`$HDB;
        .log.warn "Could not connect to ",HDB," to initiate reload";
        [.log.info "Initiating reload on ",HDB;
         h"\\l ."]];	
    }

.z.exit:{ / unexpected exit: clear, wipe TMPSAVE contents (doesn't rm the directory itself)
    if[not KOE;
        t:tables`.;t@:where 11h=type each t@\:`sym;                          
        maketmp each t;
        @[`.;t;0#];
        ]}

/ connect to ticker plant for (schema;(logcount;log))
if[.qi.isproc;
    if[(::)~HDB:.proc.self.options`hdb;
        '"A wdb process needs a hdb entry in its process config"];
    .proc.replay .proc.subscribe`];

.cron.add[`writeall;.z.p;"n"$.conf.WRITE_EVERY]
.cron.add[`memcheck;.z.p;"n"$.conf.MEM_CHECK_EVERY]
.event.addhandler[`.z.ts;`.cron.run]
\t .conf.QTIMER
