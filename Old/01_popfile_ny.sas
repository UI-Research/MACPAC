/*******************************************************************************************************************/ 
/*	Purpose: Aggregate cleaned up MAX data from 01_studypop_analyticfile with CDPS data and AHRF and MSA/HRR data 
/*			on user-input geographic variable			
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)
/* Notes:
/*		Updated full benefits definition
/*******************************************************************************************************************/ 
/*Options to change*/
%macro prod();
	options obs=MAX;
	/*Log*/
	proc printto print="P:\MCD-SPVR\log\05_addcdps_&sysdate..lst"
	               log="P:\MCD-SPVR\log\05_addcdps_&sysdate..log" NEW;
	run;
%mend;

%macro test();	
	options obs=MAX;
	/*Log*/
	proc printto;run;
%mend;

*%prod();
%test();

/*Libraries*/
libname data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;
libname cpds_wt "P:\MCD-SPVR\data\NO_PII\CDPS_WEIGHTS";
libname scores  "P:\MCD-SPVR\data\workspace\CDPS_SCORES";
libname ahrf_hrr "\\sas1_alt\MCD-SPVR\data\NO_PII\HRR\workspace";

/* Macro vars to change*/
%let indata_max = space.id_pop_15oct2019; /*input data file from 01_studypop_analyticfile*/

%let year=2012;
%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.));
%let space_name = %sysfunc(date(),date9.);
%let report_folder = P:\MCD-SPVR\reports;
* AHRF HRR-state level file -- check for updated file;
%let ahrf_hrr = ahrf_hrr_state_v09_25_2018;

/*WHAT is going on with NY-13780
proc sql;
	create table temp_ny as
	select *
	from space.temp_max_cdpsscores
	where st_msa="NY-13780";
quit;

data space.temp_ny;
	set temp_ny;
run;*/
proc contents data=space.temp_ny;run;

proc sql;
	create table temp as
	select * from space.temp_ny
	where TOT_MDCD_PYMT_AMT > 512978;
quit;
/*
proc freq data=temp;
	tables TOT_MDCD_PYMT_AMT;
run;

proc contents data=data.Maxdata_lt_2012;run;
*/
proc sql;
	create table temp_clms_lt as
	select BENE_ID, STATE_CD, DIAG_CD_1, MAX_TOS, MDCD_PYMT_AMT, SRVC_BGN_DT, SRVC_END_DT,TYPE_CLM_CD,
			. as QTY_SRVC_UNITS, /*NA for IP & LT file*/
			1 as CLM_CNT,
			SRVC_END_DT-SRVC_BGN_DT+1 as SRVC_DAYS
	from data.Maxdata_lt_2012
	where BENE_ID in (select distinct bene_id from temp);

	create table sum_clms_lt as
	select bene_id, STATE_CD, TYPE_CLM_CD,DIAG_CD_1, MAX_TOS, sum(MDCD_PYMT_AMT) as mdcd_pymt, sum(SRVC_DAYS) as sum_srvc_days
	from temp_clms_lt
	group by bene_id, STATE_CD, TYPE_CLM_CD,DIAG_CD_1, MAX_TOS;
quit;

proc freq data=sum_clms_lt;
	tables TYPE_CLM_CD*MAX_TOS*DIAG_CD_1*mdcd_pymt*sum_srvc_days/list missing;
run;


proc contents data= data.Maxdata_lt_2012;run;

proc sql;
	create table temp_clms_lt as
	select *,
			. as QTY_SRVC_UNITS, /*NA for IP & LT file*/
			1 as CLM_CNT,
			SRVC_END_DT-SRVC_BGN_DT+1 as SRVC_DAYS
	from data.Maxdata_lt_2012
	where BENE_ID in (select distinct bene_id from temp);

	create table sum_clms_lt as
	select bene_id, NPI,PRVDR_ID_NMBR,STATE_CD, TYPE_CLM_CD,DIAG_CD_1, MAX_TOS, sum(MDCD_PYMT_AMT) as mdcd_pymt, sum(SRVC_DAYS) as sum_srvc_days
	from temp_clms_lt
	group by bene_id,NPI,PRVDR_ID_NMBR,STATE_CD, TYPE_CLM_CD,DIAG_CD_1, MAX_TOS;
quit;	
proc freq data=sum_clms_lt;
	tables NPI*PRVDR_ID_NMBR*TYPE_CLM_CD*MAX_TOS*DIAG_CD_1*mdcd_pymt*sum_srvc_days/list missing;
run;

proc freq data=sum_clms_lt;
	tables NPI*PRVDR_ID_NMBR/list missing;
run;


proc sql;
	create table temp_clms_ip as
	select BENE_ID, STATE_CD, DIAG_CD_1, MAX_TOS, MDCD_PYMT_AMT, SRVC_BGN_DT, SRVC_END_DT,
			. as QTY_SRVC_UNITS, /*NA for IP & LT file*/
			1 as CLM_CNT,
			SRVC_END_DT-SRVC_BGN_DT+1 as SRVC_DAYS
	from data.Maxdata_ip_2012
	where BENE_ID in (select distinct bene_id from temp);

	create table sum_clms_ip as
	select bene_id, STATE_CD, DIAG_CD_1, MAX_TOS, sum(MDCD_PYMT_AMT) as mdcd_pymt, sum(SRVC_DAYS) as sum_srvc_days
	from temp_clms_ip
	group by bene_id, STATE_CD, DIAG_CD_1, MAX_TOS;
quit;

proc freq data=sum_clms_ip;
	tables MAX_TOS*DIAG_CD_1*mdcd_pymt*sum_srvc_days/list missing;
run;
libname  stateot "P:\MCD-SPVR\data\raw_data\SAS_DATASETS\OT";
proc sql;
	create table temp_clms_ot1 as
	select BENE_ID, STATE_CD, DIAG_CD_1, MAX_TOS, MDCD_PYMT_AMT, SRVC_BGN_DT, SRVC_END_DT,
			. as QTY_SRVC_UNITS, /*NA for IP & LT file*/
			1 as CLM_CNT,
			SRVC_END_DT-SRVC_BGN_DT+1 as SRVC_DAYS
	from stateot.Maxdata_ny_ot_2012_001
	where BENE_ID in (select distinct bene_id from temp);
	/*no claims from second file
	create table temp_clms_ot2 as
	select STATE_CD, DIAG_CD_1, MAX_TOS, MDCD_PYMT_AMT, SRVC_BGN_DT, SRVC_END_DT,
			. as QTY_SRVC_UNITS,
			1 as CLM_CNT,
			SRVC_END_DT-SRVC_BGN_DT+1 as SRVC_DAYS
	from stateot.Maxdata_ny_ot_2012_002
	where BENE_ID in (select distinct bene_id from temp);
	*/
	create table sum_clms_ot as
	select bene_id, STATE_CD, DIAG_CD_1, MAX_TOS, sum(MDCD_PYMT_AMT) as mdcd_pymt, sum(SRVC_DAYS) as sum_srvc_days
	from temp_clms_ot1
	group by bene_id, STATE_CD, DIAG_CD_1, MAX_TOS;
quit;

proc freq data=sum_clms_ot;
	tables MAX_TOS*DIAG_CD_1*mdcd_pymt*sum_srvc_days/list missing;
run;

proc freq data=temp_clms_lt;
	tables MAX_TOS DIAG_CD_1 MDCD_PYMT_AMT/list missing;
run;
proc freq data=temp_clms_ip;
	tables MAX_TOS DIAG_CD_1 MDCD_PYMT_AMT/list missing;
run;
proc freq data=temp_clms_ot1;
	tables MAX_TOS DIAG_CD_1 MDCD_PYMT_AMT/list missing;
run;

proc freq data=space.temp_ny;
	tables replaced_cnty replaced_zip/list missing;
	where TOT_MDCD_PYMT_AMT > 512978;
run;


proc freq data=space.temp_ny;
	tables bene_id*el_dob*el_sex_cd/list missing;
	format bene_id $missing_char. el_dob missing_num.;
	where TOT_MDCD_PYMT_AMT > 512978;
run;

proc sql;
	select count(*) label="Distinct Bene ID"
	from (select distinct bene_id from space.temp_ny)
	where TOT_MDCD_PYMT_AMT > 512978;
quit;

proc sql;
	create table temp_ny_oldzip as
	select *
	from space.temp_personlevelphp
	where bene_id in (select distinct bene_id from temp);
quit;

proc freq data=temp_ny_oldzip;
	tables EL_RSDNC_ZIP_CD_LTST;
run;

proc freq data=temp;
	tables zipcode;
run;
