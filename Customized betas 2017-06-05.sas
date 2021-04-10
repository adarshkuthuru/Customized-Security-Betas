proc datasets lib=work kill nolist memtype=data;
quit;

data SB.Cust_final;
	set SB.final;
run;

	data SB.Cust_final;
		set SB.Cust_final;
		date1=intnx("month",endmonth,0,"E");
		format date1 date9.;
	run;

proc sort data=SB.Cust_final; by date1 beta; run;


*indexing by end of month date & estimating total companies on that date;
data SB.Cust_final;
		set SB.Cust_final;
		by date1;
		if first.date1 then nrow=1;
  		else nrow+1;
	run;

proc sql;
	create table SB.Cust_final as
	select distinct *,max(nrow) as max_nrow
	from SB.Cust_final
	group by date1;
quit;

proc sort data=SB.Cust_final; by date1 nrow; run;


*segregating mid and end datasets;
data SB.Cust_final_ends SB.Cust_final_mid;
  set SB.Cust_final;
  if nrow<=25 then output SB.Cust_final_ends;
  else if max_nrow-nrow<25 then output SB.Cust_final_ends;
  else output SB.Cust_final_mid;
run;

*creating portfolios for mid dataset;
data SB.Cust_final_mid;
  set SB.Cust_final_mid;
  low=nrow-25;
  high=nrow+25;
run; 
proc sql;
	create table SB.Cust_final_mid1 as
	select distinct a.permno,a.date1,a.nrow,a.max_nrow,a.low,a.high,b.nrow as new_nrow,b.permno as port_perm,b.beta
	from SB.Cust_final_mid as a,SB.Cust_final as b
	where a.date1=b.date1 and a.low <= b.nrow <= a.high
  	order by a.Date1,a.nrow;
quit;

*creating portfolios for end dataset;
data SB.Cust_final_ends;
  set SB.Cust_final_ends;
  if nrow<=25 then low =1;
  else low =max_nrow-50;
  if nrow>25 then high=max_nrow;
  else high=51;
run; 
proc sql;
	create table SB.Cust_final_ends1 as
	select distinct a.permno,a.date1,a.nrow,a.max_nrow,a.low,a.high,b.nrow as new_nrow,b.permno as port_perm,b.beta
	from SB.Cust_final_ends as a,SB.Cust_final as b
	where a.date1=b.date1 and a.low <= b.nrow <= a.high
  	order by a.Date1,a.nrow;
quit;



*Method 1: average of betas;
*estimating average beta at the end of each month;
proc sql;
	create table SB.method3 as
	select distinct a.date1,a.nrow,mean(a.Beta) as PortBeta
	from SB.Cust_final_mid1 as a
	group by date1, nrow;
quit;

proc sql;
	create table SB.method31 as
	select distinct a.date1,a.nrow,mean(a.Beta) as PortBeta
	from SB.Cust_final_ends1 as a
	group by date1, nrow;
quit;

*merging dataset with portfolio betas;
proc sql;
	create table SB.portbeta3c as
	select distinct a.*,b.portbeta 
	from SB.Cust_final_mid1 as a, SB.method3 as b
	where a.date1=b.date1 and a.nrow=b.nrow
	order by a.date1,a.nrow,a.beta;
quit;
proc sql;
	create table SB.portbeta3c1 as
	select distinct a.*,b.portbeta 
	from SB.Cust_final_ends1 as a, SB.method31 as b
	where a.date1=b.date1 and a.nrow=b.nrow
	order by a.date1,a.nrow,a.beta;
quit;


*Regressing monthly excessreturns on betas, across all portfolios;
*regression data set;
proc sql;
	create table SB.portbeta3 as
	select distinct a.*,b.monthlyret
	from SB.portbeta3c as a, SB.Monthlyreturns as b
	where a.permno=b.permno and year(a.date1)=year(b.endmonth) and month(a.date1)=month(b.endmonth)-1
	order by a.permno,a.date1;
quit;

proc sql;
	create table SB.portbeta3a as
	select distinct a.*,b.monthlyrf,a.monthlyret-b.monthlyrf as exret
	from SB.portbeta3 as a, FF1 as b
	where year(a.date1)=year(b.endmonth) and month(a.date1)=month(b.endmonth)-1
	order by a.permno,a.date1;
quit;


proc sql;
	create table SB.portbeta32 as
	select distinct a.*,b.monthlyret
	from SB.portbeta3c1 as a, SB.Monthlyreturns as b
	where a.permno=b.permno and year(a.date1)=year(b.endmonth) and month(a.date1)=month(b.endmonth)-1
	order by a.permno,a.date1;
quit;

proc sql;
	create table SB.portbeta3a1 as
	select distinct a.*,b.monthlyrf,a.monthlyret-b.monthlyrf as exret
	from SB.portbeta32 as a, FF1 as b
	where year(a.date1)=year(b.endmonth) and month(a.date1)=month(b.endmonth)-1
	order by a.permno,a.date1;
quit;

data SB.portbeta33;
	set SB.portbeta3a SB.portbeta3a1;
run;



proc sort data=SB.portbeta3a; by date1 permno new_nrow; run;

proc reg data=SB.portbeta33 noprint tableout outest=Alpha4;
  by date1;
  model exret = portbeta;
quit;

data Alpha4;
  set Alpha4;
  where _Type_ in ('PARMS','T');
  keep date1 _Type_ PortBeta;
  rename PortBeta=BetaCoeff;
  rename _Type_ =Stat;
run;

data SB.Result3;
  set Alpha4;
run;


*Method 2: Pooled regression;

data SB.Cust_final_mid1a;
	set SB.Cust_final_mid1;
/*	if date1<='31DEC1980'd;*/
    if date1 ='31DEC2015'd; *dataset1;
/*  if date1 <= '31DEC2010'd;*/
/*  if date1 >= '20JAN2006'd; *dataset2;*/
/*  if date1 <= '31DEC2005'd;*/
/*  if date1 >= '20JAN2001'd;  *dataset3;*/
/*    if date1 <= '31DEC2000'd;*/
/*    if date1 >= '20JAN1996'd; *dataset4;*/
/*	if date1 <= '31DEC1995'd;*/
/*    if date1 >= '20JAN1991'd;  *dataset5;*/
/*      if date1 <= '31DEC1990'd;*/
/*      if date1 >= '20JAN1986'd;*/
/*	if date1 <= '31DEC1985'd;*/
/*    if date1 >= '20JAN1981'd;*/
	past6mdate=intnx("month",Date1,-6,"E");
	format past6mdate date9.;
run;

proc sql;
	create table SB.method4 as
	select distinct a.*,b.date,b.ret
	from SB.Cust_final_mid1a as a,SB.Dailystockdata as b
	where a.port_perm=b.permno and a.past6mdate<b.date<=a.date1
	order by a.permno,a.date1,a.new_nrow,b.date;
quit;

proc sql;
	create table method4 as
	select distinct a.*,b.mkt_rf,b.rf,a.ret-b.rf as exret
	from method4 as a,ff as b
	where a.date=b.date
	order by a.permno,a.date1,a.date;
quit;
