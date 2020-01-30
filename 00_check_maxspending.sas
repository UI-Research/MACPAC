/*******************************************************************************************************************/ 
/*	Purpose: Check MAX spending categories	
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak
/*		1) Collapse macros for easier manipulation
/*	Notes: 
/*******************************************************************************************************************/ 
libname  data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname  space   "P:\MCD-SPVR\data\workspace";
libname  stateot "P:\MCD-SPVR\data\raw_data\SAS_DATASETS\OT";

%let indata=data.maxdata_ps_2012; /*incoming data*/

proc contents data=&indata.;run;

options obs=MAX;

proc sql;
 	select name into :prem_cols separated by ", "
	from dictionary.columns
	where libname="DATA" and memname="MAXDATA_PS_2012" and name like "PREM_MDCD_PYMT_AMT_%";
quit;
proc sql;
	select name into :ffs_cols separated by ", "
	from dictionary.columns
	where libname="DATA" and memname="MAXDATA_PS_2012" and name like "FFS_PYMT_AMT_%";
quit;

proc sql;
	select state_cd, 
		sum(sum(&ffs_cols.)) format=comma16.2 as sum_tos, 
		sum(TOT_MDCD_FFS_PYMT_AMT) format=comma16.2 as ff_pd,
		sum(sum(&prem_cols.)) format=comma16.2 as sum_prem,
		sum(TOT_MDCD_PREM_PYMT_AMT) format=comma16.2 as prem_pd,
		sum(TOT_MDCD_PYMT_AMT)  format=comma16.2 as tot_mdcd
	from &indata.
	where bene_id in (select distinct bene_id from space.pop_cdps_scores)
	group by state_cd;
quit;
