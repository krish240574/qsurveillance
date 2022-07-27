//-- CONFIG -------------

/ TODO :
/ Check removedups code - not working
shp:{[a]c:count a;$[98h=type a;[a:value a 0;c,shp a];[$[0<=type a;c,shp a 0;""]]]}


// database to write to 
dbdir:`:hdb

// directory to read the files from
inputdir:`:examplecsv

// the number of bytes to read at once, used by .Q.fsn
chunksize:`int$100*2 xexp 20;

// compression parameters
/ .z.zd:17 2 6

//-- END OF CONFIG ------

// maintain a dictionary of the db partitions which have been written to by the loader
partitions:()!()

// maintain a list of files which have been read
filesread:()

// the column names that we want to read in
columnnames:`index`systemtime`midpoint`spread`buys`sells,raze {`$raze each string x,/:til 15}each raze enlist each("bd";"bn";"bcn";"bln";"bmn";"ad";"an";"acn";"aln";"amn")
colStr:"IP",154#"F"

// function to print log info
out:{-1(string .z.z)," ",x}

// loader function
loaddata:{[filename;rawdata]

 symbol:first "." vs last "/" vs string filename; 
 show symbol;
 
 out"Reading in data chunk";
 
 // check if we have already read some data from this file
 // if this is the first time we've seen it, then the first row
 // contains the header information, so we want to load it accounting for that
 // in both cases we want to return a table with the same column names
 data:delete index from data:([]sym:(count data)#`$symbol),
 'data:$[filename in filesread; 
 [flip columnames!(colStr;enlist",")0:rawdata; filesread,::filename]; columnnames xcol (colStr;enlist",")0:rawdata];

 out"Read ",(string count data)," rows";

 // enumerate the table - best to do this once
 out"Enumerating";
 data:.Q.en[dbdir;data];  

 show "Hours:";
 show distinct `hh$data`systemtime;

 // write out data to each date partition
 {[data;hour]
  // sub-select the data to write
  towrite:select from data where hour=`hh$systemtime;
  
  // generate the write path
  writepath:.Q.par[dbdir;hour;`$"trade/"];
  out"Writing ",(string count towrite)," rows to ",string writepath;
   
  // splay the table - use an error trap
  .[upsert;(writepath;towrite);{out"ERROR - failed to save table: ",x}]; 
  
  // make sure the written path is in the partition dictionary
  partitions[writepath]:hour;
 
  }[data] each exec distinct systemtime.hh from data;
 } 

// set an attribute on a specified column
// return success status
setattribute:{[partition;attrcol;attribute] .[{@[x;y;z];1b};(partition;attrcol;attribute);0b]}

// set the partition attribute (sort the table if required)
sortandsetp:{[partition;sortcols]
 
 out"Sorting and setting `p# attribute in partition ",string partition;
 
 // attempt to apply an attribute.
 // the attribute should be set on the first of the sort cols
 parted:setattribute[partition;first sortcols;`p#];
 
 // if it fails, resort the table and set the attribute
 if[not parted;
    out"Sorting table";
    sorted:.[{x xasc y;1b};(sortcols;partition);{out"ERROR - failed to sort table: ",x; 0b}];
    // check if the table has been sorted
    if[sorted;
       // try to set the attribute again after the sort
       parted:setattribute[partition;first sortcols;`p#]]];
 
 // print the status when done
 $[parted; out"`p# attribute set successfully"; out"ERROR - failed to set attribute"];
 }

// build an hourly table
hourlystatsfromtrade:{[path;hour]
  
 out"Building hourly stats for hour ",(string hour)," and path ",string path;

 / build the hourly stats 
 select spread:spread, buys:buys, sells:sells, vol:bmn0+amn0, obi:(bmn0-amn0)%(bmn0+amn0) by systemtime.hh from get path}
 / need to build n% ask volume and n% bid volume here 
 / https://blog.kaiko.com/api-tutorial-how-to-use-market-depth-to-study-cryptocurrency-order-book-dynamics-62ed823a0aaa 
 /select high:max price,low:min price, open:first price, close:last price,volume:sum size by date:date,sym from  get path}

buildhourlystats:{[removedups]
 
 out"**** Building hourly stats table ****";
 
 // make sure we have an up-to-date sym file
 sym::get hsym `$(string dbdir),"/sym";
 
 // get the stats
 partitions::get `:partitions;
 stats:0!raze hourlystatsfromtrade'[key partitions; value partitions];
 
 out"Created ",(string count stats)," hourly stat rows";
 
 // create the path to the hourly table
 hourlypath:hsym`$(string dbdir),"/hourly/";
 
 // enumerate it
 out"Enumerating hourly table";
 .Q.en[dbdir; stats];

 // remove duplicates
/ if[removedups; 
/  dups:exec i from stats where ([]date;sym) in @[{select date,sym from get x};hourlypath;([]date:();sym:())];
/  $[count dups;
/    [out"Removed ",(string count dups)," duplicates from stats table";
/     stats:select from stats where not i in dups];
/    out"No duplicates found"]];
 
 // save the data
 if[count stats;
  out"Saving to hourly table";
  if[.[{x upsert y;1b};(hourlypath;stats);{out"ERROR - failed to save hourly table: ",x;0b}];
   // make sure the table is sorted by date with an attribute on it
   sortandsetp[hourlypath;`hh]]];
 }

finish:{[buildhourly]
 // re-sort and set attributes on each partition
 /sortandsetp[;`sym`systemtime] each key partitions;

 // build daily stats, removing duplicates
 if[buildhourly; buildhourlystats[1b];]; 
 }

// load all the files from a specified directory
loadallfiles:{[dir;buildhourly]
 show "Inside loadallfiles:";
 show dir;
  
 // get the contents of the directory
 /filelist:key dir:hsym dir;
 
 // create the full path
 /filelist:` sv' dir,'filelist;
 
 // Load each file in chunks
 /out"**** LOADING ",(string x)," ****";
 /.Q.fsn[loaddata[x];x;chunksize]} each filelist;
 
 // finish the load
 finish[buildhourly];
 }

/ loadallfiles[inputdir;1b]
loadallfiles[`$"/home/krishna/Downloads/kafka/surveillance/kafka-csv-producer-master/tmp/examplecsv";1b]

