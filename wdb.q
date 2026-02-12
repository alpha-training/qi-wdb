/ adapted from https://github.com/simongarland/tick/blob/master/w.q

getTMPSAVE:{`$":../tmp.",(string .z.i),".",string x}  
TMPSAVE:getTMPSAVE .z.d
MAXROWS:100000
KEEPONEXIT:any`keeponexit`koe in key .qi.opts

append:{[t;data]
    t insert data;
    if[MAXROWS<count value t;
        / append enumerated buffer to disk
        .[` sv TMPSAVE,t,`;();,;.Q.en[`:.]`. t]; 
        / clear buffer
        @[`.;t;0#]; 
        ]}
        
upd:append

disksort:{[t;c;a] 
    if[not`s~attr(t:hsym t)c;
        if[count t;
            ii:iasc iasc flip c!t c,:();
            if[not$[(0,-1+count ii)~(first;last)@\:ii;@[{`s#x;1b};ii;0b];0b];
                {v:get y;if[not$[all(fv:first v)~/:256#v;all fv~/:v;0b];v[x]:v;y set v];}[ii]each` sv't,'get` sv t,`.d]];
        @[t;first c;a]];t}

.u.end:{ / end of day: save, clear, sort on disk, move, hdb reload
    t:tables`.;t@:where 11h=type each t@\:`sym;
    / append enumerated buffer to disk
    {.[` sv TMPSAVE,x,`;();,;.Q.en[`:.]`. x]}each t;
    / clear buffer
    @[`.;t;0#];
    / sort on disk by sym and set `p#
    / {@[`sym xasc` sv TMPSAVE,x,`;`sym;`p#]}each t;
    {disksort[` sv TMPSAVE,x,`;`sym;`p#]}each t;
    / move the complete partition to final home, use <mv> instead of built-in <r> if filesystem whines
    system"r ",(1_string TMPSAVE)," ",-1_1_string .Q.par[`:.;x;`];
    / reset TMPSAVE for new day
    TMPSAVE::getTMPSAVE .z.d;	
    / and notify hdb to reload and pick up new partition
    $[null h:.ipc.conn`$HDB;
        .log.warn "Could not connect to ",HDB," to initiate reload";
        [.log.info "Initiating reload on ",HDB;
         h"\\l ."]];	
    }

.z.exit:{ / unexpected exit: clear, wipe TMPSAVE contents (doesn't rm the directory itself)
    if[not KEEPONEXIT;
        t:tables`.;t@:where 11h=type each t@\:`sym;
        / clear buffer                          
        @[`.;t;0#];
        / overwrite written-so-far-today data with empty
        {.[` sv TMPSAVE,x,`;();:;.Q.en[`:.]`. x]}each t;
        ]}

/ connect to ticker plant for (schema;(logcount;log))
if[.qi.isproc;
    if[(::)~HDB:.proc.self.options`hdb;
        '"A wdb process needs a hdb entry in its process config"];
    .proc.replay .proc.subscribe`];