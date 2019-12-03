/*******************************************************************************************************************/ 
/*	Purpose: Aggregate cleaned up MAX data from 01_studypop_analyticfile with CDPS data and AHRF and MSA/HRR data 
/*			on user-input geographic variable			
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)
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
	options obs=100000;
	/*Log*/
	proc printto;run;
%mend;

%prod();
*%test();

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
%let indata_max = space.id_pop_29oct2019; /*input data file from 01_studypop_analyticfile*/
%let outdata=space.temp_max_cdpsscores;
%let year=2012;
%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.));
%let space_name = %sysfunc(date(),date9.);
%let report_folder = P:\MCD-SPVR\reports;
* AHRF HRR-state level file -- check for updated file;
%let ahrf_hrr = ahrf_hrr_state_v09_25_2018;

/*drop tables for space*/
proc sql;
	drop table space.temp_max_cdpsscores, space.temp_ahrf_aggre, space.temp_ahrf_msa_xwalk;
quit;
/****************/
/* Add CDPS data*/
/****************/
*1 concatenate state cdps files (DATA);
data work.cdps_allst;
	set scores.cdps_asth:;
	state_cd=substr(recipno,1,2);
run;

*2 join CDPS to categories_full by RECIPNO (SQL);
proc sql;
	create table cat_plus_cdps as
	select *
	from &indata_max. a left join cdps_allst (drop= male age state_cd) b 
	where a.EL_RSTRCT_BNFT_FLG_LTST = "1";
quit;
         
proc sql;
	create table space.drp_benes_wofllbenf as
	select *
	from &indata_max.
	where EL_RSTRCT_BNFT_FLG_LTST ne "1";
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
	create table indata_max as
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

/************************************************/
/*Initial processing to attach MSA info to files*/
/************************************************/
proc sql;
	create table space.temp_ahrf_msa_xwalk as
	select *,
		&year. as year, 
		catx("-",state_cd,county_fips) as st_cnty,
		case when state_fips = '72' then 'PR' 
			else state_cd 
			end as state_cd_fx,
		case when metropolitanmicropolitanstatis = 'Metropolitan Statistical Area' then catx('-', state_cd, cbsacode)
			else catx('-', state_cd, "XXXXX")
			end as st_msa,
		case when metropolitanmicropolitanstatis ne 'Metropolitan Statistical Area' then "Non-metropolitan area"
			else cbsatitle
		end as cbsatitle_fx,
		case when missing(unemp_d) or missing(unemp_n) then 1 else 0 end as ahrf_msg
	from space.ahrf_msa;
quit;		
proc sql;
	create table space.temp_ahrf_aggre as
	select year, st_msa, 
		case when substr(st_msa,4,5) = "XXXXX" then "Non-metropolitan area"
			else cbsatitle
		end as cbsatitle_fx,
		1000*sum(hos_n)/sum(pop)label = "Number of hospital beds per 1k people, 2010" as beds, 
		1000*sum(md_n)/sum(pop)label = "Number of physicians per 1k people, 2010" as md, 
		sum(poverty_d)/sum(poverty_n) label = "Rate of persons in poverty" as povrate,
		sum(unemp_d)/sum(unemp_n) label = "Unemployment rate" as urate,
		sum(ahrf_msg) as sum_ahrf_msg
	from space.temp_ahrf_msa_xwalk 
	group by year, st_msa, calculated cbsatitle_fx;
quit;

proc sql ;
	create table max_2012_msa_join (drop=cbsatitle_fx)as
	select  a.*, b.* ,cbsatitle_fx as cbsatitle
	from indata_max a left join space.temp_ahrf_msa_xwalk (drop=year) b
	on a.county=b.st_cnty;
quit;

proc freq data=max_2012_msa_join;
	title "Obs with ST_MSA matches";
	tables st_msa county/list missing;
	format st_msa county $missing_char.;
run;

proc sql;
	title Obs without ST_MSA matches;
	select county, count(county) as number_missing
	from max_2012_msa_join
	where st_msa = ' '
	group by county;
	title;
quit;

proc sql;
	create table &outdata. as
	select *
	from max_2012_msa_join
	where st_msa ne ' ';
quit;

