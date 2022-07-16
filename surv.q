 / c:`systemtime`midpoint`spread`buys`sells,raze {`$raze each string x,/:til 15}each raze enlist each("bd";"bn";"bcn";"bln";"bmn";"ad";"an";"acn";"aln";"amn")
 data:({x,y}/){colStr:"P",154#"F";([]ticker:((count t)#x)),'t:(colStr;enlist ",")0:`$":../tmp/",(string x),".csv"}each`ADA`ETH`BTC
 save `:data.csv
 
 
