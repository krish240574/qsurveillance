 / c:`systemtime`midpoint`spread`buys`sells,raze {`$raze each string x,/:til 15}each raze enlist each("bd";"bn";"bcn";"bln";"bmn";"ad";"an";"acn";"aln";"amn")
 data:({x,y}/){colStr:"P",154#"F";([]ticker:((count t)#x)),'t:(colStr;enlist ",")0:`$":../tmp/",(string x),".csv"}each`ADA`ETH`BTC
 data:({x,y}/){colStr:"P",154#"F";([]ticker:((count t)#first "." vs last "/" vs string x)),'t:(colStr;enlist ",")0:x}each ` sv '(hsym `$idir),'key hsym `$idir:"/home/krishna/Downloads/kafka/surveillance/kafka-csv-producer-master/tmp/examplecsv"
 save `:data.csv
