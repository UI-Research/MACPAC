/*******************************************************************************************************************/ 
/*	Purpose: Using a yearly MAX input data set, create an analytic file with specified variables. 
/*				Fix invalid geographic information.
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)
/*	Notes: 
/*		Drop benes with partial Medicaid eligibility when cell type is determined (last observed month). Add a summary variable on the number of total partial benefit months in the cell.
/*		Include small cell size for internal purposes
/*		Include top-coded spending summary series (sum, mean, etc.)
/*		1) Collapse macros for easier manipulation
/*		2) Dependent on MAX data formatting
/*******************************************************************************************************************/ 
/*Options to change*/
%macro prod();
	options obs=MAX;
	/*Log*/
	proc printto print="P:\MCD-SPVR\log\02_geofix_&sysdate..lst"
	               log="P:\MCD-SPVR\log\02_geofix_&sysdate..log" NEW;
	run;
%mend;

%macro test();	
	options obs=MAX;
	/*Log*/
	proc printto;run;
%mend;

%prod();
*%test();

* Macro variables for processing;
%let indata= space.temp_personlevel;
%let outdata=space.temp_personlevel_geofixed;

%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.)); /*date+time for file names, to ensure files aren't overwritten*/
%let space_name = %sysfunc(date(),date9.); /*date for naming convention in space lib*/
%let indata_year = 2012; /*claims year*/
%let age_date = 12,01,2012; /*date from which to calculate age. must be month,day,year*/
%let dum_tab = %str(ods excel options(sheet_interval="table"); ods exclude all; data _null_; file print; put _all_; run; ods select all;);
%let report_folder = P:\MCD-SPVR\reports;

/*libraries*/
libname data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;

/*drop the table we will create here for space reasons*/
proc sql;
	drop table &outdata.;
quit;


ods excel file="&report_folder.\geofix_summary_&fname..xlsx";
ods excel options(sheet_name="geo fix summary" sheet_interval="none");

proc sql;
	create table max_zip_merge as
	select a.zip_code, a.st_cnty,b.*, b.st_cnty as max_st_cnty,
		(a.zip_code = .) as no_zip_match,
		(a.st_cnty = ' ') as no_cnty_match,
		(b.zip_miss = a.zip_code) as zip_match,
		(b.st_cnty = a.st_cnty) as cnty_match,
		case 
			when b.zip_miss in (select distinct zip_code from area.all_zips) then 1
			when b.zip_miss = . then .
			else 0
		end as valid_zip, /*1 = MAX zip in all_zips data, . = MAX zip is missing, 0 = MAX zip is not in all_zips data*/
		case 
			when b.st_cnty in (select distinct st_cnty from area.all_zips) then 1
			when b.st_cnty = ' ' then .
			else 0
		end as valid_cnty /*1 = MAX county in all_zips data, . = MAX county is missing, 0 = MAX county is not in all_zips data*/
	from area.all_zips a full join &indata. b
	on a.zip_code = b.zip_miss and a.st_cnty = b.st_cnty /*need to match on both bc these either var alone many-to-many match*/
	where not missing(b.st_cnty) or not missing(b.zip_miss);
quit;

proc odstext;
  p "Max and Zip Data Merge Results";
  p "first two cols: 0 = no match between MAX and Zips, 1 = match between MAX and Zips";
  p "second two cols: 0 = MAX var not in Zips data, 1 = MAX var in Zips data, . = MAX var is missing";
run;

proc freq data=max_zip_merge;
	title "Max and Zip Data Merge Results";
	title2 "first two cols: 0 = no match between MAX and Zips, 1 = match between MAX and Zips";
	title3 "second two cols: 0 = MAX var not in Zips data, 1 = MAX var in Zips data, . = MAX var is missing";
	tables no_zip_match*no_cnty_match*valid_zip*valid_cnty/list missing nopercent;
run;

/**************************************/
/*Start work on problem zips/counties*/
/**************************************/
proc sql;
	create table invalid_zip as 
	select *
	from max_zip_merge
	where (valid_zip in (0, .) /*invalid or missing zips*/ and valid_cnty = 1 /*with valid counties*/) or 
			(valid_zip = 1 and valid_cnty = 1 and no_zip_match = 1 and no_cnty_match = 1) /*invalid zip/county combos - judgement call here to replace the zips for these obs*/
	order by max_st_cnty;
quit;

proc sql;
	create table validzip_validcnty as
	select * 
	from max_zip_merge
	where valid_zip = 1 and valid_cnty = 1 and no_zip_match = 0 and no_cnty_match = 0
	order by zip_miss;
quit;

proc sql;
	create table validzip_invalidcnty as
	select * 
	from max_zip_merge
	where valid_zip = 1 and valid_cnty in (0,.)
	order by zip_miss;
quit;

/**************************************/
/*Fix invalid zips with valid counties*/
/**************************************/
proc sql; /*need a data set with all possible strata only*/
	create table invalidzip_validcnty_df as
	select st_cnty, zip_code
	from area.all_zips
	where st_cnty in (select distinct max_st_cnty from invalid_zip)
	order by st_cnty;
quit;

proc sql;
	create table zip_strata as
	select max_st_cnty as st_cnty, count(max_st_cnty) as _nsize_
	from invalid_zip
	group by max_st_cnty;
quit;

/*select random zip codes based on county*/
/*this will generate notes because sample size is greater than sampling unit - that is necessary for our needs*/
proc surveyselect data=invalidzip_validcnty_df noprint 
      method=urs 
      n=zip_strata /*data set containing stratum sample sizes*/
      seed=1953
      out=random_zips;
   strata st_cnty;
run;

*reorganize file;
data zip_long;
	set random_zips; /*de-flatten random_zips*/
	do i = 1 to NumberHits;
	   output;
	end;
run;

proc sort data=zip_long (keep=st_cnty zip_code);
	by st_cnty;
run;

data invalid_zip_replaced;
	merge invalid_zip zip_long;
run;

/********************************/
/*Fix zips with invalid counties*/
/********************************/
proc sql; /*need a data set with all possible strata only*/
	create table validzip_invalidcnty_df as
	select st_cnty, zip_code
	from area.all_zips
	where zip_code in (select distinct zip_miss from validzip_invalidcnty)
	order by zip_code;
quit;

proc sql;
	create table cnty_strata as
	select zip_miss as zip_code, count(zip_miss) as _nsize_
	from validzip_invalidcnty
	group by zip_miss;
quit;

/*select random zip codes based on county*/
/*this will generate notes because sample size is greater than sampling unit - that is necessary for our needs*/
proc surveyselect data=validzip_invalidcnty_df noprint 
      method=urs 
      n=cnty_strata /*data set containing stratum sample sizes*/
      seed=1953
      out=random_cnty;
   strata zip_code;
run;

*reorganize file;
data cnty_long;
	set random_cnty; /*de-flatten random_zips*/
	do i = 1 to NumberHits;
	   output;
	end;
run;

proc sort data=cnty_long (keep=st_cnty zip_code);
	by zip_code;
run;

proc odstext;
  p "After Invalid Zip Fixes";
run;

data invalid_cnty_replaced;
	merge validzip_invalidcnty cnty_long;
run;
/*************************************/
/*merge new zip file with valid zips;*/
/*************************************/
data max_zip_complete;
	set invalid_zip_replaced invalid_cnty_replaced validzip_validcnty;
run;

proc odstext;
  p "Zip Fix Summary";
run;

proc sql;
	create table &outdata. as
	select *,
		case
			when valid_zip in (0,.) or (no_zip_match = 1 and no_cnty_match = 1) then zip_code
			when valid_zip = 1 then zip_miss
			else .
		end as zip_fx label="Zip code",
		case
			when valid_zip in (0,.) or (no_zip_match = 1 and no_cnty_match = 1) then 1
			when valid_zip = 1 then 0
			else .
		end as replaced_zip label="1 if zip was replaced during geo fixes",
		case
			when valid_cnty in (0,.)then st_cnty
			when valid_cnty = 1 then max_st_cnty
			else ' '
		end as cnty_fx format=$8. label="State abbreviation and county code",
		case
			when valid_cnty in (0,.)then 1
			when valid_cnty = 1 then 0
			else .
		end as replaced_cnty label="1 if state/county was replaced during geo fixes"
	from max_zip_complete;

	create table check_zip_fx as
	select replaced_zip, replaced_cnty, no_zip_match, no_cnty_match,valid_zip,valid_cnty,
		(b.zip_fx = a.zip_code) as zip_match, 
		(b.cnty_fx = a.st_cnty) as cnty_match
	from area.all_zips a right join &outdata. b
	on a.zip_code = b.zip_fx and a.st_cnty = b.cnty_fx
	where not missing(b.cnty_fx) or not missing(b.zip_fx);
quit;
proc odstext;
  p "Max and Zip Data Merge Results";
run;

proc freq data=check_zip_fx;
	title "Max and Zip Data Merge Results";
	tables zip_match*cnty_match*replaced_zip*replaced_cnty*valid_zip*valid_cnty/list missing;
run;
title;

proc freq data=&outdata.;
	title "Zip and County Code Fix Frequencies";
	tables zip_fx cnty_fx;
	format zip_fx missing_zip. cnty_fx $missing_char.; 
run;
title;
ods excel close;

