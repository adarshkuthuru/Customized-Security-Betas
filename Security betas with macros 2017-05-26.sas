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
	date1=date;
	past6mdate1=past6mdate;
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
	date1=date;
	past6mdate1=past6mdate;
	nrow=_N_;
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

*Macros to calculate firm betas;
proc sql noprint;
        select count(*) into :mon from Monthlydata;
quit;

options symbolgen;
%macro doit;
%do i=1 %to &mon;

	proc sql;
     select permno into :pm from Monthlydata where nrow=&i;
    quit;

	proc sql;
     select date1 into :dt from Monthlydata where nrow=&i;
    quit;

	proc sql;
     select past6mdate1 into :dt125 from Monthlydata where nrow=&i;
    quit;

	data new;
		set Dailystockdata;
		if permno=&pm;
		if date<=&dt;
		if date>&dt125; 
	run;

	*combining above two data sets;
	proc sql;
  		create table new as
  		select distinct a.*,b.Mkt_RF,b.rf,a.ret-b.rf as Exret
  		from new as a, ff as b
  		where a.date1=b.date1
  		order by a.permno,a.date1;
	quit;


	proc sort data=new; by permno date1; run;

	proc reg data=new noprint tableout outest=Alpha;
  		by permno past6mdate1;
  		model Exret= MKT_RF;
	quit;

	data Beta;
  		set Alpha;
  		where _Type_ in ('PARMS');
  		keep permno past6mdate1 _Type_ Mkt_RF;
  		if _Type_='PARMS' then Beta=Mkt_RF;
  		rename Mkt_RF=Beta;
  		label Mkt_RF=Beta;
  		rename _Type_ =Stat;
	run;

	proc append data=beta base=final; run;

%end;
%mend doit;
%doit






*Calculation of firm-wise monthly returns;
proc sql;
	create table Monthlyreturns as
	select distinct a.permno,a.endmonth,sum(a.ret) as Monthlyret
	from dailystockdata1 as a
	group by a.permno,a.endmonth;
quit;

*combining above two data sets;
proc sql;
  create table Monthlyreturns as
  select distinct a.*,b.Monthlyrf,a.monthlyret-b.Monthlyrf as Exret
  from Monthlyreturns as a, ff1 as b
  where a.endmonth=b.endmonth
  order by a.permno,a.endmonth;
quit;


/*past 6-month data*/

proc sql;
  create table DailyStockData2 as
  select distinct a.permno,a.past6mdate,b.date,a.endmonth,b.Ret
  from DailyStockData1 as a, DailyStockData1 as b
  where a.permno=b.permno and a.past6mdate<b.Date<=a.endmonth and missing(b.Ret)=0 
  order by a.endmonth,b.Date,a.permno;
quit; 

/*combining with FF factors*/
proc sql;
  create table DailyStockData3 as
  select distinct a.*,b.rf,b.Mkt_RF
  from DailyStockData2 as a, FF as b
  where a.date=b.date 
  order by a.permno,a.Date,a.endmonth;
quit; 

data DailyStockData3;
	set DailyStockData3;
	exret=ret-rf;
run;

/*proc means data=DailyStockData3 noprint;*/
/*  by permno endmonth;*/
/*  var ExRet Mkt_rf;*/
/*  output out=Meanret mean=ExRet Mkt_rf;*/
/*quit;*/

proc sort data=DailyStockData3; by permno endmonth; run;
proc reg data=DailyStockData3 noprint tableout outest=Alpha;
  by permno endmonth;
  model ExRet= MKT_RF;
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


/*Arranging betas in ascending order and forming portfolios */
proc sort data=Beta; by endmonth beta; run;


proc rank data=Beta groups=40 out=Beta_pf;
  by endmonth;
  var Beta;
  ranks Rank_Beta;
run;
data Beta_pf;
  set Beta_pf;
  Rank_Beta=Rank_Beta+1;
run;


*Method 1: Average of betas;
proc sql;
	create table method1 as
	select distinct a.endmonth,a.Rank_Beta,mean(a.Beta) as PortBeta
	from Beta_pf as a
	group by endmonth, Rank_Beta;
quit;

*Assigning portfolio beta to all stocks in the portfolio;

proc sql;
	create table portbeta1 as
	select distinct a.*,b.portbeta 
	from Beta_pf as a, method1 as b
	where a.endmonth=b.endmonth and a.Rank_beta=b.Rank_beta
	order by a.endmonth,a.beta;
quit;

*Regressing monthlyreturns on betas;

*regression data set;
proc sql;
	create table portbeta1 as
	select distinct a.*,b.exret
	from portbeta1 as a, Monthlyreturns as b
	where a.permno=b.permno and year(a.endmonth)=year(b.endmonth) and month(a.endmonth)=month(b.endmonth)-1
	order by a.permno,a.endmonth;
quit;

/*proc sort data=portbeta1; by AnomalyVar Rank_Anomaly; run;*/
proc reg data=portbeta1 noprint tableout outest=Alpha1;
  model exret = portbeta;
quit;

data Alpha1;
  set Alpha1;
  where _Type_ in ('PARMS','T');
  keep _Type_ PortBeta;
  rename PortBeta=BetaCoeff;
  rename _Type_ =Stat;
run;


*Method 2: Pooled regression;


**common part for methods 2&3: starts;
proc sql;
	create table method2 as
	select distinct a.permno,a.endmonth,sum(a.Mkt_RF) as MktRfMonthly,sum(a.exret) as exretmonthly
	from Dailystockdata3 as a
	group by a.endmonth, a.permno;
quit;

proc sql;
	create table method2 as
	select distinct a.*,b.MktRFMonthly,b.exretmonthly
	from Beta_pf as a,method2 as b
	where a.endmonth=b.endmonth and a.permno=b.permno
	order by a.permno,a.endmonth;
quit;


**common part for methods 2&3: ends;

*Method 2, step 2: pooled regression by portfolio;

proc sort data=method2; by endmonth Rank_Beta ; run;

proc reg data=method2 noprint tableout outest=Alpha2;
  by endmonth Rank_Beta;
  model exretmonthly = MktRfMonthly;
quit;

data Alpha2;
  set Alpha2;
  where _Type_ in ('PARMS');
  keep endmonth Rank_Beta _Type_ MktRfMonthly;
  rename MktRfMonthly=PortBeta;
  rename _Type_ =Stat;
run;


*Assigning portfolio beta to all stocks in the portfolio;

proc sql;
	create table portbeta2 as
	select distinct a.*,b.portbeta 
	from Beta_pf as a, Alpha2 as b
	where a.endmonth=b.endmonth and a.Rank_beta=b.Rank_beta
	order by a.endmonth,a.beta;
quit;


*Regressing monthlyreturns on betas;

*regression data set;
proc sql;
	create table portbeta2 as
	select distinct a.*,b.exret
	from portbeta2 as a, Monthlyreturns as b
	where a.permno=b.permno and year(a.endmonth)=year(b.endmonth) and month(a.endmonth)=month(b.endmonth)-1
	order by a.permno,a.endmonth;
quit;

/*proc sort data=portbeta1; by AnomalyVar Rank_Anomaly; run;*/
proc reg data=portbeta2 noprint tableout outest=Alpha2A;
  model exret = portbeta;
quit;

data Alpha1;
  set Alpha1;
  where _Type_ in ('PARMS','T');
  keep _Type_ PortBeta;
  rename PortBeta=BetaCoeff;
  rename _Type_ =Stat;
run;

****************************************************************************
