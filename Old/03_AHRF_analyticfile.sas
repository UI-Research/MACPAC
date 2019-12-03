/* 
   Create a dataset with desired MAX variables for given cells at the  county and national level,
    based on latest eligible month, and merge with AHRF county-level data

   Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)

*/

* Date for version control;
*%let date=10_30_2018;
options obs=1000;
* log;
*PROC PRINTTO PRINT="P:\MCD-SPVR\log\02_max_ahrf_&sysdate..lst"
               LOG="P:\MCD-SPVR\log\02_max_ahrf_&sysdate..log" NEW;
*RUN;

libname data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;


data ahrf_msa_xwalk;
	set space.ahrf_msa;
	year=2012;
	if state_fips in('72') then state_cd='PR';
	if metropolitanmicropolitanstatis in('Metropolitan Statistical Area') then 
	   st_msa = catx('-', state_cd, cbsacode);
	else do; 
		st_msa = catx('-', state_cd, "XXXXX");
		cbsatitle="Non-Metro-Rest-of-State";
		end;
label st_msa="State-MSA Code";
run;

proc sql;
	create table ahrf_msa_2012 as
	select  
		state_cd, 
		st_msa, 
		cbsatitle,
		1000*sum(hos_n)/sum(pop) as beds, 
		1000*sum(md_n)/sum(pop) as md, 
		sum(poverty_d)/sum(poverty_n) as povrate,
		sum(unemp_d)/sum(unemp_n) as urate, 
		0 as _ahrf_msg
	from ahrf_msa_xwalk
	group by year, state_cd, st_msa, cbsatitle;
quit;
