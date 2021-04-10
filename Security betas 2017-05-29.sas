proc datasets lib=work kill nolist memtype=data;
quit;

libname SB 'F:\Local Disk F\Security betas\Data'; run;

PROC IMPORT OUT= WORK.FF
            DATAFILE= "F:\Local Disk F\Security betas\Data\FF_Research_Data_Factors_daily.xlsx"
            DBMS=EXCEL REPLACE;
     RANGE="Sheet1$";
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;



data DailyStockData;
  set SB.DailyStockRET_FF2008;
  past6mdate=intnx("month",Date,-6,"E"); *t-6 month end date at the start of date;
  if shrcd in (10,11);
  format past6mdate date9.;
  drop shrcd permco prc shrout;
run;

data DailyStockData;
  set DailyStockData;
  if missing(ret)=1 then delete;
run;


*number values of dates;
data DailyStockData;
  set DailyStockData;
/*	date1=date;*/
/*	past6mdate1=past6mdate;*/
	endmonth=intnx("month",date,0,"E");
	format endmonth date9.;
run;

data SB.DailyStockData;
  set DailyStockData;
run;

********************************************
*****************Monthly Data***************;

/*Extracting month end dates from daily data*/

proc sql;
	create table Monthlydata as
	select distinct *,max(day(date)) as Day
	from DailyStockData as a
	group by permno,past6mdate;
quit;

data Monthlydata;
	set Monthlydata;
	if day(date)=Day;
	drop Day;
run;

*number values of dates;
data Monthlydata;
	set Monthlydata;
	nrow=_N_;
	endmonth=intnx("month",Date,0,"E");
	format endmonth date9.;
run;

data SB.Monthlydata;
	set Monthlydata;
run;

*Monthly ff factors;

data ff;
	set ff;
	endmonth=intnx("month",Date,0,"E");
	date1=date;
	format endmonth date9.;
run;


proc sql;
	create table ff1 as
	select distinct a.endmonth,sum(a.rf) as Monthlyrf
	from ff as a
	group by a.endmonth;
quit;

**Subsetting the data;

data Monthlydata1;
  set Monthlydata;
/*  if past6mdate >='20JUL2010'd; *dataset1;*/
/*  if date <= '31DEC2010'd;
  if date >= '20JAN2006'd;*/ *dataset2;
/*  if date <= '31DEC2005'd;*/
/*  if date >= '20JAN2001'd;*/  *dataset3;
/*    if date <= '31DEC2000'd;*/
/*    if date >= '20JAN1996'd;*/ *dataset4;
/*	if date <= '31DEC1995'd;*/
/*    if date >= '20JAN1991'd;*/  *dataset5;
/*      if date <= '31DEC1990'd;*/
/*      if date >= '20JAN1986'd;*/
/*	if date <= '31DEC1985'd;*/
/*    if date >= '20JAN1981'd;*/
      if date <= '31DEC1980'd;
      if date >= '20JAN1976'd;
run;


/*past 6-month data*/

proc sql;
  create table DailyStockData1 as
  select distinct a.permno,a.date,a.ret,b.date as endmonth,b.past6mdate
  from DailyStockData as a, Monthlydata1 as b
  where a.permno=b.permno and b.past6mdate<a.Date<=b.date and missing(a.Ret)=0 
  order by a.permno,b.past6mdate,a.Date;
quit; 


	*combining above two data sets;
	proc sql;
  		create table DailyStockData1 as
  		select distinct a.*,b.Mkt_RF,b.rf,a.ret-b.rf as Exret
  		from DailyStockData1 as a, ff as b
  		where a.date=b.date
  		order by a.permno,a.past6mdate,a.date;
	quit;



	proc sort data=DailyStockData1; by permno past6mdate date; run;

	proc reg data=DailyStockData1 noprint tableout outest=Alpha;
  		by permno endmonth;
  		model Exret= MKT_RF;
	quit;

	data Beta;
  		set Alpha;
  		where _Type_ in ('PARMS');
  		keep permno endmonth _Type_ Mkt_RF;
  		if _Type_='PARMS' then Beta=Mkt_RF;
  		rename Mkt_RF=Beta;
  		label Mkt_RF=Beta;
  		rename _Type_ =Stat;
	run;

	proc append data=beta base=final; run;

	proc sort data=final; by permno endmonth; run;

	data SB.final;
		set final;
	run;




/*Arranging betas in ascending order and forming portfolios */
	data final;
		set final;
		date1=intnx("month",endmonth,0,"E");
		format date1 date9.;
	run;
proc sort data=final; by date1 beta; run;


proc rank data=final groups=100 out=Beta_pf;
  by date1;
  var Beta;
  ranks Rank_Beta;
run;

data Beta_pf;
  set Beta_pf;
  Rank_Beta=Rank_Beta+1;
run;

data SB.Beta_pf;
  set Beta_pf;
run;

*Method 1: Average of betas;
proc sql;
	create table SB.method1 as
	select distinct a.date1,a.Rank_Beta,mean(a.Beta) as PortBeta
	from SB.Beta_pf as a
	group by date1, Rank_Beta;
quit;

*Assigning portfolio beta to all stocks in the portfolio;

proc sql;
	create table SB.portbeta1 as
	select distinct a.*,b.portbeta 
	from SB.Beta_pf as a, SB.method1 as b
	where a.date1=b.date1 and a.Rank_beta=b.Rank_beta
	order by a.date1,a.beta;
quit;


/*Estimating monthly returns*/

proc sql;
	create table Monthlyreturns as
	select distinct permno,endmonth,sum(ret) as monthlyret
	from DailyStockData as a
	group by permno,endmonth;
quit;

data SB.Monthlyreturns;
  set Monthlyreturns;
run;


*Regressing monthly excessreturns on betas, across all portfolios;
*regression data set;
proc sql;
	create table SB.portbeta1 as
	select distinct a.*,b.monthlyret
	from SB.portbeta1 as a, SB.Monthlyreturns as b
	where a.permno=b.permno and year(a.date1)=year(b.endmonth) and month(a.date1)=month(b.endmonth)-1
	order by a.permno,a.date1;
quit;

proc sql;
	create table SB.portbeta1 as
	select distinct a.*,b.monthlyrf,a.monthlyret-b.monthlyrf as exret
	from SB.portbeta1 as a, FF1 as b
	where year(a.date1)=year(b.endmonth) and month(a.date1)=month(b.endmonth)-1
	order by a.permno,a.date1;
quit;



proc sort data=SB.portbeta1; by date1 permno; run;

proc reg data=portbeta1 noprint tableout outest=Alpha1;
  by date1;
  model exret = portbeta;
quit;


data Alpha1;
  set Alpha1;
  where _Type_ in ('PARMS','T');
  keep date1 _Type_ PortBeta;
  rename PortBeta=BetaCoeff;
  rename _Type_ =Stat;
run;

data SB.Result1;
  set Alpha1;
run;

*Ask how many coeff of beta estimates should be there?;

*Method 2: Pooled regression;




***********
**new edit;
**common part for methods 2&3: starts;
data Beta_pf1;
	set Beta_pf;
/*	if date1<='31DEC1980'd;*/
    if date1 >='20JAN2010'd; *dataset1;
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
	create table method2a as
	select distinct a.*,b.date,b.ret
	from Beta_pf1 as a,Dailystockdata as b
	where a.permno=b.permno and a.past6mdate<b.date<=a.date1
	order by a.permno,a.date1,b.date;
quit;

proc sql;
	create table method2a as
	select distinct a.*,b.mkt_rf,b.rf,a.ret-b.rf as exret
	from method2a as a,ff as b
	where a.date=b.date
	order by a.permno,a.date1,a.date;
quit;

**common part for methods 2&3: ends;
*Method 2, step 2: pooled regression by portfolio;

proc sort data=method2a; by date1 Rank_Beta ; run;

proc reg data=method2a noprint tableout outest=Alpha2;
  by date1 Rank_Beta;
  model exret = Mkt_Rf;
quit;

data Alpha2;
  set Alpha2;
  where _Type_ in ('PARMS');
  keep date1 Rank_Beta _Type_ Mkt_Rf;
  rename Mkt_Rf=PortBeta;
  rename _Type_ =Stat;
run;

	proc append data=Alpha2 base=final1; run;

	proc sort data=final1; by date1 Rank_beta; run;

	data SB.final1;
		set final1;
	run;

*Assigning portfolio beta to all stocks in the portfolio;

proc sql;
	create table portbeta2 as
	select distinct a.*,b.portbeta 
	from Beta_pf as a, final1 as b
	where a.date1=b.date1 and a.Rank_beta=b.Rank_beta
	order by a.date1,a.Rank_beta;
quit;

*Regressing monthlyreturns on betas;

*regression data set;
proc sql;
	create table portbeta2 as
	select distinct a.*,b.monthlyret
	from portbeta2 as a, Monthlyreturns as b
	where a.permno=b.permno and year(a.date1)=year(b.endmonth) and month(a.date1)=month(b.endmonth)-1
	order by a.permno,a.date1;
quit;

proc sql;
	create table portbeta2 as
	select distinct a.*,b.monthlyrf,a.monthlyret-b.monthlyrf as exret
	from portbeta2 as a, FF1 as b
	where year(a.date1)=year(b.endmonth) and month(a.date1)=month(b.endmonth)-1
	order by a.permno,a.date1;
quit;


proc sort data=portbeta2; by date1 permno; run;

proc reg data=portbeta2 noprint tableout outest=Alpha3;
  by date1;
  model exret = portbeta;
quit;

data Alpha3;
  set Alpha3;
  where _Type_ in ('PARMS','T');
  keep date1 _Type_ PortBeta;
  rename PortBeta=BetaCoeff;
  rename _Type_ =Stat;
  label PortBeta=BetaCoeff;
run;

data SB.Result2;
  set Alpha3;
run;
****************************************************************************
