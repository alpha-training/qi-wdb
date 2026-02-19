/ adapted from https://github.com/simongarland/tick/blob/master/w.q
.qi.import`event
.qi.import`cron

KOE:any`keeponexit`koe in key .qi.opts
gettmppath:{hsym`$$[`tmpPath in key .conf;.conf.tmpPath;.conf.DATA,"/tmp"],"/wdb.",string[.z.i],".",string x}
TMPPATH:gettmppath .z.d
HDBPATH:":",.conf.DATA,"/HDB/"
partition:HDBPATH,string .z.d
writetmp:{.[` sv TMPPATH,x,`;();,;.Q.en[`$HDBPATH]`. x]} / have a updtmp and clear function
clearall:{@[`.;tables`;0#]}
writeandclear:{writetmp each tables`;clearall`}
writeall:{-1"moving tables out of memory and onto disk at: ",(8#2_string .z.n)," UKT";writeandclear`}
memcheck:{if[.conf.WDB_MAXMB<first system["w"]%1024*1024;writeandclear`]}
hdb:.qi.getconf[`hdb;"hdb"]

append:{[t;data]
    if[t in tables`;t insert data;
     if[.conf.MAXROWS<count get t;writeandclear`]]
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
    writeandclear`;
    {disksort[` sv TMPPATH,x,`;`sym;`p#]}each tables`; /sort on disk by sym and set `p#;
    system.qi.mv," ",(1_string TMPPATH)," ",1_partition;
    TMPPATH::gettmppath .z.d;
    partition::HDBPATH,string .z.d;
    .Q.gc`;	
    $[null h:.ipc.conn`$HDB;
        .log.warn "Could not connect to ",HDB," to initiate reload";
        [.log.info "Initiating reload on ",HDB;
         h"\\l ."]];	
    } / need some pattern matching to do for each wdb file like .z.d. what if wdb goes down and we join back in on the day

.z.exit:{if[not KOE;writeandclear`]} 

/ connect to ticker plant for (schema;(logcount;log))
.wdb.init:{
    if[(::)~HDB:.proc.self.options`hdb;
        '"A wdb process needs a hdb entry in its process config"];
    .proc.replay .proc.subscribe`;
    .cron.add[`writeall;.z.p;.conf.WRITE_EVERY];
    .cron.add[`memcheck;.z.p;.conf.MEM_CHECK_EVERY];
    .event.addhandler[`.z.ts;`.cron.run];
    .cron.start[];
 }
