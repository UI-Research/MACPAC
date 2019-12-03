/*******************************************************************************************************************/ 
/*	Purpose: Compare my data process results with previous data processing results			
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)
/*	Notes: 
/*		1) Collapse macros for easier manipulation
/*******************************************************************************************************************/ 
/*Libraries*/
libname data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;
libname cpds_wt  "P:\MCD-SPVR\data\NO_PII\CDPS_WEIGHTS";
libname  scores  "P:\MCD-SPVR\data\workspace\CDPS_SCORES";
/*Options to change*/
options obs=MAX;

/* Macro vars to change*/
%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.));
%let space_name = %sysfunc(date(),date9.);
%let indata_max = space.id_pop_10jan2019; /*input data file from 01_studypop_analyticfile*/
%let year = 2012;


proc printto;run;
proc compare base=space.hrr_2012_24dec2018 compare=out.hrr_state_allcells_v09282018 printall;
   title 'Comparing Two Data Sets: Full Report';
run;

proc freq data=space.msa_2012_26dec2018;
	tables st_msa*cell_age*state_cd*year*cbsatitle*mc_cat*dis_cat*ltss_cat*age_cat*dual_cat*foster_cat /list ;
run;

proc freq data=space.hrr_2012_24dec2018;
	tables hrrnum*cell_age*state_cd*year*mc_cat*dis_cat*ltss_cat*age_cat*dual_cat*foster_cat /list ;
run;

%macro age_levels(indata=,indata_msg=,outdata=);
	proc sql;
		create table &outdata._child as
		select *
		from &indata.
		where age_cat=1;

		create table &outdata._child_msg as
		select *
		from &indata_msg.
		where age_cat=1;

		create table &outdata._adult as
		select *
		from &indata.
		where age_cat=2;

		create table &outdata._adult_msg as
		select *
		from &indata_msg.
		where age_cat=2;

		create table &outdata._senior as
		select *
		from &indata.
		where age_cat=3;

		create table &outdata._senior_msg as
		select *
		from &indata_msg.
		where age_cat=3;
	quit;
%mend;
/******************************************************************************/
%age_levels(indata=space.id_pop_09jan2019,indata_msg=space.id_pop_dropped09jan2019,outdata=personlevel)


proc freq data=space.id_pop_dropped09jan2019;
	tables cell*age/list missing;
	where cell="chip" and age_cat = 2;
run;

proc sql;
	select count(*) 
	from personlevel_adult_msg;
quit;

proc sql;
	create table counts as
	select count(*) as child_count format=comma12.,
		(select count(*) from personlevel_child_msg) as child_msg_count format=comma12.,
		(select count(*) from personlevel_adult) as adult_count format=comma12.,
		(select count(*) from personlevel_adult_msg) as adult_msg_count format=comma12.,
		(select count(*) from personlevel_senior) as senior_count format=comma12.,
		(select count(*) from personlevel_senior_msg) as senior_msg_count format=comma12.
	from personlevel_child;
	title Totals for Valid and Invalid Obs by Age and Total;
	select child_count, child_msg_count, 
		(1-((child_count-child_msg_count)/child_count))*100 as child_percent_msg, 
		adult_count, adult_msg_count, 
		(1-((adult_count-adult_msg_count)/adult_count))*100 as adult_percent_msg, 
		senior_count, senior_msg_count, 
		(1-((senior_count-senior_msg_count)/senior_count))*100 as senior_percent_msg, 
		sum(child_count, child_msg_count,adult_count,adult_msg_count,senior_count,senior_msg_count) as total format=comma12.
	from counts;
	title;
quit;

/*look at BOE timing vs. age (age calculated as of 12/01/2012)*/
/*these don't match up well*/
proc freq data=personlevel_adult;
	tables MAX_ELG_CD_MO_12*EL_MAX_ELGBLTY_CD_LTST/list missing;
run;
 missing(age_cat) or missing(cell_age) or missing(dual_cat) or missing(mc_cat) or missing(dis_cat) or missing(ltss_cat);
/*look at why adults in cell 08 are missing*/
proc sql;
	select count(*) as total_recs, count(age_cat) as age_cat_present, count(cell_age) as cell_age_present, count(dual_cat) as dual_cat_present, count(mc_cat) as mc_cat_present, 
		count(dis_cat) as dis_cat_present, count(ltss_cat) as ltss_cat_present, count(ltss) as ltss_present
	from personlevel_adult_msg;
quit;

proc format;
	value chip_elig
		. = 'Missing'
		2 = 'Eligibile'
		3 = 'Eligible'
		other = 'Not Eligible or Unknown';
quit;
proc freq data=personlevel_adult_msg;
	tables EL_CHIP_FLAG_1*EL_CHIP_FLAG_2*EL_CHIP_FLAG_3*EL_CHIP_FLAG_4*EL_CHIP_FLAG_5*EL_CHIP_FLAG_6*EL_CHIP_FLAG_7*EL_CHIP_FLAG_8*EL_CHIP_FLAG_9*EL_CHIP_FLAG_10*EL_CHIP_FLAG_11*EL_CHIP_FLAG_12/list missing;
	format EL_CHIP_FLAG_1 EL_CHIP_FLAG_2 EL_CHIP_FLAG_3 EL_CHIP_FLAG_4 EL_CHIP_FLAG_5 EL_CHIP_FLAG_6 EL_CHIP_FLAG_7 EL_CHIP_FLAG_8 EL_CHIP_FLAG_9 EL_CHIP_FLAG_10 EL_CHIP_FLAG_11 EL_CHIP_FLAG_12 chip_elig.;
run;



/*Reason #1 why adults were dropped: they were assigned a chip flag at some point*/
proc sql noprint;
	select name || 'in (2,3)' into :in_condition separated by " or "
	from dictionary.columns
	where libname="WORK" and memname = upcase("personlevel_adult_msg") and name like upcase("el_chip_flag_%");
quit;

%put &in_condition;

proc sql;
	create table bad_chip_assign as
	select *
	from personlevel_adult_msg
	where &in_condition;
quit;

proc freq data=bad_chip_assign;
	title "Age vs. CHIP indicator";
	tables age /missing;
run;


proc freq data=personlevel_adult_msg;
	tables month_1;
run;
