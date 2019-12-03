/*******************************************************************************************************************/ 
/*	Purpose: Create summary tables for the prevalence of certain CDPS flags			
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak
/*	Notes: 
/*		1) Collapse macros for easier manipulation
/*******************************************************************************************************************/ 
/*Options to change*/
%macro prod();
	options obs=MAX;
	/*Log*/
	proc printto print="P:\MCD-SPVR\log\04_cdps_prev_&sysdate..lst"
	               log="P:\MCD-SPVR\log\04_cdps_prev_&sysdate..log" NEW;
	run;
%mend;

%macro test();	
	options obs=1000000;
	/*Log*/
	proc printto;run;
%mend;

%prod();
/*%test();*/

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
%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.));
%let space_name = %sysfunc(date(),date9.);
%let indata_max = space.id_pop_25feb2019; /*input data file from 01_studypop_analyticfile*/
%let year = 2012;
%let dum_tab = %str(ods excel options(sheet_interval="table"); ods exclude all; data _null_; file print; put _all_; run; ods select all;);
%let report_folder = P:\MCD-SPVR\reports;
* AHRF HRR-state level file -- check for updated file;
%let ahrf_hrr = ahrf_hrr_state_v09_25_2018;

/****************/
/* Add CDPS data*/
/****************/
%macro cdps;
	*1 concatenate state cdps files (DATA);
	data work.cdps_allst;
		set scores.cdps_:;
		state_cd=substr(recipno,1,2);
	run;

	*2 join CDPS to categories_full by RECIPNO (SQL);
	proc sql;
		create table cat_plus_cdps as
		select *
		from &indata_max. a left join cdps_allst (drop= male age state_cd) b 
		on a.recipno = b.RECIPNO;
	quit;

	*3 get means of spending and CDPS and generate mult_c=(spend_c/cdps_c) (by 'chip','nmcd','','msg') (SQL);
	proc sql;
		create table spendavg as
		select age_servicetype, 
			AVG(TOT_MDCD_PYMT_AMT) as mspend_c,
			AVG(CDPS_SCORE) as cdps_c,
			AVG(TOT_MDCD_PYMT_AMT) / AVG(CDPS_SCORE) as mult_c
		from cat_plus_cdps
		group by age_servicetype;
	quit;

	*4. join means to individual records by 'chip','nmcd','','msg'  and 
	   generate pspend_i=spend_i*mult_c and rspend_i=spend_i-pspend_i (SQL);
	proc sql;
		create table space.max2012_cdps as
		select T1.*,
			(T1.TOT_MDCD_PYMT_AMT) AS mspend_i,
			T2.mspend_c,
			T2.cdps_c,
			T2.mult_c,
			T1.CDPS_SCORE*T2.mult_c AS Pspend_i,
			(T1.TOT_MDCD_PYMT_AMT) - T1.CDPS_SCORE*T2.mult_c AS Rspend_i
		from cat_plus_cdps T1 left join spendavg T2
		on T1.age_servicetype=T2.age_servicetype;
	quit;
%mend;
%cdps;
/*
proc sql;
	create table child as
	select *
	from space.max2012_cdps 
	where age_cat = 1;

	create table adult as
	select *
	from space.max2012_cdps 
	where age_cat = 2;

	create table senior as
	select *
	from space.max2012_cdps
	where age_cat = 3;
quit;

%macro cdps_sumstats(agecat_dat);
	proc sql;
		select cell_type1, cell_type2, cell_type3, cell_type4, age_servicetype, 
			sum(cell_n) as cell_n, 
	        sum(case when NOCDPS=0 then 1 else . end) as cdps_n, 
			(sum(case when NOCDPS=0 then 1 else . end)/sum(cell_n))*100 as cdps_pct,

			sum(DIA2L) as DIA2L_n,
			(sum(DIA2L)/sum(cell_n))*100 as DIA2L_pct_all,
			(sum(DIA2L)/sum(case when NOCDPS=0 then 1 else . end))*100 as DIA2L_pct_cdps,

			sum(DIA2M) as DIA2M_n,
			(sum(DIA2M)/sum(cell_n))*100 as DIA2M_pct_all,
			(sum(DIA2M)/sum(case when NOCDPS=0 then 1 else . end))*100 as DIA2M_pct_cdps,

			sum(DIA2M) as DIA2M_n,
			(sum(DIA2M)/sum(cell_n))*100 as DIA2M_pct_all,
			(sum(DIA2M)/sum(case when NOCDPS=0 then 1 else . end))*100 as DIA2M_pct_cdps,

			sum(CAREL) as CAREL_n,
			(sum(CAREL)/sum(cell_n))*100 as CAREL_pct_all,
			(sum(CAREL)/sum(case when NOCDPS=0 then 1 else . end))*100 as CAREL_pct_cdps,

			sum(PULL) as PULL_n,
			(sum(PULL)/sum(cell_n))*100 as PULL_pct_all,
			(sum(PULL)/sum(case when NOCDPS=0 then 1 else . end))*100 as PULL_pct_cdps,

			sum(PSYL) as PSYL_n,
			(sum(PSYL)/sum(cell_n))*100 as PSYL_pct_all,
			(sum(PSYL)/sum(case when NOCDPS=0 then 1 else . end))*100 as PSYL_pct_cdps,

			sum(CANL) as CANL_n,
			(sum(CANL)/sum(cell_n))*100 as CANL_pct_all,
			(sum(CANL)/sum(case when NOCDPS=0 then 1 else . end))*100 as CANL_pct_cdps,

			sum(PRGCMP) as PRGCMP_n,
			(sum(PRGCMP)/sum(cell_n))*100 as PRGCMP_pct_all,
			(sum(PRGCMP)/sum(case when NOCDPS=0 then 1 else . end))*100 as PRGCMP_pct_cdps,

			sum(HIVM) as HIVM_n,
			(sum(HIVM)/sum(cell_n))*100 as HIVM_pct_all,
			(sum(HIVM)/count(NOCDPS))*100 as HIVM_pct_cdps,

			sum(AIDSH) as AIDSH_n,
			(sum(AIDSH)/sum(cell_n))*100 as AIDSH_pct_all,
			(sum(AIDSH)/sum(case when NOCDPS=0 then 1 else . end))*100 as AIDSH_pct_cdps,

			sum(CERL) as CERL_n,
			(sum(CERL)/sum(cell_n))*100 as CERL_pct_all,
			(sum(CERL)/sum(case when NOCDPS=0 then 1 else . end))*100 as CERL_pct_cdps
		from &agecat_dat.
		group by cell_type1, cell_type2, cell_type3, cell_type4, cell, age_servicetype
		order by age_servicetype;
	quit;
%mend;


ods excel file="&report_folder.\cdps_flags_&fname..xlsx";
ods excel options(sheet_name="child" sheet_interval="none");
	%cdps_sumstats(agecat_dat=child);
	&dum_tab.;

	ods excel options(sheet_name="adult" sheet_interval="none");
	%cdps_sumstats(agecat_dat=adult);
	&dum_tab.;

	ods excel options(sheet_name="senior" sheet_interval="none");
	%cdps_sumstats(agecat_dat=senior);
ods excel close;

proc freq data=indata_max;
	table NOCDPS;
run;
*/
