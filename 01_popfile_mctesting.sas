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
	proc printto print="P:\MCD-SPVR\log\01_popfile_&sysdate..lst"
	               log="P:\MCD-SPVR\log\01_popfile_&sysdate..log" NEW;
	run;
%mend;

%macro test();	
	options obs=MAX;
	/*Log*/
	proc printto;run;
%mend;

*%prod();
%test();

* Macro variables for processing;
%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.)); /*date+time for file names, to ensure files aren't overwritten*/
%let space_name = %sysfunc(date(),date9.); /*date for naming convention in space lib*/
%let indata_year = 2012; /*claims year*/
%let age_date = 12,01,2012; /*date from which to calculate age. must be month,day,year*/
%let indata=data.maxdata_ps_2012; /*incoming data*/
%let outdata=space.temp_personlevel&space_name.; /*final data table for this step*/

/*libraries*/
libname data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;

proc sql;
	create table space.temp_mc as
	select orig.state_cd, orig.mc_cat as orig_mc_cat, new.mc_cat as new_mc_cat, orig.mo_mc as orig_mo_mc, new.mo_mc as new_mo_mc
	from space.temp_max_cdpsscores orig left join space.temp_personlevelphp new
	on orig.bene_id=new.bene_id and orig.state_cd = new.state_cd;
quit;

proc sql;
	select state_cd, 
		count(*) as total_enr label="All Enrollees" format=comma16.,
		sum(case when orig_mc_cat = 1 then 1 else 0 end) as orig_mc_cat label="Old definition" format=comma16., 
		(sum(case when orig_mc_cat = 1 then 1 else 0 end)/count(*)) as orig_mc_cat_perc label="Old definition (%)"  format=percent.,
		sum(case when new_mc_cat  = 1 then 1 else 0 end) as new_mc_cat  label="New definition" format=comma16.,
		(sum(case when new_mc_cat = 1 then 1 else 0 end)/count(*)) as new_mc_cat_perc label="New definition (%)"  format=percent.,
		(sum(case when new_mc_cat = 1 then 1 else 0 end)/count(*))-(sum(case when orig_mc_cat = 1 then 1 else 0 end)/count(*)) label="New-Old Difference (%)" format=percent.
		from space.temp_mc
	where state_cd is not null
	group by state_cd;
quit;

proc sql;
	select state_cd, 
		sum(orig_mo_mc) as orig_mo_mc label="Old definition" format=comma16., 
		sum(new_mo_mc) as new_mo_mc  label="New definition" format=comma16.,
		sum(new_mo_mc)-sum(orig_mo_mc) label="New-Old Difference (%)" format=comma16.
		from space.temp_mc
	where state_cd is not null
	group by state_cd;
quit;

proc freq data=space.temp_mc;
	tables orig_mo_mc*new_mo_mc/list missing;
	where state_cd = "PA";
run;

proc sql;
	select count(*)
	from space.temp_max_cdpsscores;
quit;

proc contents data=space.temp_personlevelphp;run;

proc freq data=space.temp_personlevelphp;
	tables state_cd*EL_PHP_MO_12*mc_12*MC_COMBO_MO_12/list missing;
	where state_cd = "PA";
run;

proc sql;
	select sum(new_mo_mc),sum(orig_mo_mc)
	from space.temp_mc
	where state_cd = "PA";
quit;
