/*******************************************************************************************************************/ 
/*	Purpose: Evaluate PS file data quality for CDPS chronic conditions flag subpopulations			
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)
/*	Notes: 
/*		1) Collapse macros for easier manipulation
/*******************************************************************************************************************/ 
/*Options to change*/
%macro prod();
	options obs=MAX;
	/*Log*/
	proc printto print="P:\MCD-SPVR\log\03_aggregate_&sysdate..lst"
	               log="P:\MCD-SPVR\log\03_aggregate_&sysdate..log" NEW;
	run;
%mend;

%macro test();	
	options obs=10000000;
	/*Log*/
	proc printto;run;
%mend;

/*%prod();*/
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

%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.));
%let space_name = %sysfunc(date(),date9.);
%let year = 2012;
%let dum_tab = %str(ods excel options(sheet_interval="table"); ods exclude all; data _null_; file print; put _all_; run; ods select all;);
%let report_folder = P:\MCD-SPVR\reports;


proc format library=library;
	value missing_S
		.S = 'Too small - marked missing'
		. = 'Missing'
		other = 'Not Missing';
	value missing_zip
		. = 'Missing'
		00000 = 'All zeros'
		99999 = 'All nines'
		other = 'Not Missing';
	value $missing_county
		' ' = 'Missing'
		'000' = 'All zeros'
		'999' = 'All nines'
		other = 'Not Missing';
	value missing_num
		. = 'Missing'
		other = 'Not Missing';
	value $missing_char
		' ' = 'Missing'
		other = 'Not Missing';
	value ltss 0="No LTSS"
			   1="LTSS"
			   9="ALL (Foster Care)";
	value dual 0="Medicaid Only"
			   1="Dual-Eligible"
			   9="ALL (Foster Care)";
	value dis  0="No Disability"
			   1="Disability"
			   9="ALL (Foster Care)";
	value mc   0="Fee-for-Service"
			   1="Managed Care"
			   9="ALL (Foster Care)";
	value age  1="Child"
			   2="Adult"
			   3="Elderly";
	value foster 1="Foster Care"
				 0="Non-Foster";
quit;


proc sql;
	create table max_cdps_dia2l as
	select a. *, b.TOT_IP_STAY_CNT, b.TOT_IP_DAY_CNT_STAYS, b.FFS_PYMT_AMT_01, (b.FFS_CLM_CNT_08 + b.FFS_CLM_CNT_12) as phys_clin_claims, 
		(b.FFS_PYMT_AMT_08 + b.FFS_PYMT_AMT_12) as phys_clin_spending, b.FFS_CLM_CNT_16, b.FFS_PYMT_AMT_16,
		case when age_servicetype = "adult_17" or age_servicetype="senior_17" then 1 /*this will be fixed in the 01_ program after 02/25*/
		else age_cat
		end as age_cat_fx,
		case when age_servicetype = "adult_17" or age_servicetype="senior_17" then "child_17" /*this will be fixed in the 01_ program after 02/25*/
		else age_servicetype
		end as age_servicetype_fx
	from space.max_cdps_dia2l a left join data.maxdata_ps_2012 b
	on a.bene_id = b.bene_id
	where dia2l = 1 and substr(age_servicetype, length(age_servicetype)-1,2) in ("05","06","07","08");
quit;

proc sql;
	create table max_lt_cdps_dia2l as
	select a.*, b.MSIS_TOS
	from max_cdps_dia2l a left join data.maxdata_lt_2012 b
	on a.bene_id = b.bene_id;
quit;
			/*
Inpatient stays (TOT_IP_STAY_CNT)
Inpatient days (TOT_IP_DAY_CNT_STAYS)
Inpatient spending (medicaid) (FFS_PYMT_AMT_01)
Physician and clinic (combined) claims (FFS_CLM_CNT_08+FFS_CLM_CNT_12)phys_clin_claims
			phys_clin_spending
Physician and clinic (combined) spending (FFS_PYMT_AMT_08+FFS_PYMT_AMT_12)
Prescription drug claims(FFS_CLM_CNT_16)
Prescription drug spending (FFS_PYMT_AMT_16)
*/

proc sql;
		select age_servicetype, 
			sum(cell_n) as cell_n, 
			sum(TOT_MDCD_PYMT_AMT) as spending format=16.,
			mean(TOT_MDCD_PYMT_AMT) as mean_spending,
			median(TOT_MDCD_PYMT_AMT) as median_spending,
			min(TOT_MDCD_PYMT_AMT) as min_spending,
			max(TOT_MDCD_PYMT_AMT) as max_spending,
			std(TOT_MDCD_PYMT_AMT) as std_spending,
			nmiss(TOT_MDCD_PYMT_AMT) as nmiss_spending
			from max_2012_msa_join
	      	group by age_servicetype;
	quit;

	data &outdata.;
		set &outdata;
		label
			cell_type1="Medicaid Only, Dual, or Foster Care"
			cell_type2="MC, FFS, or Foster Care"
			cell_type3="Disability, No Disability, or Foster Care"
			cell_type4="LTSS, No LTSS, or Foster Care"
			cell="MAS/BOE/Foster Care Category"
			age_cat="Age Category"
			cell_n="Number of Beneficiaries"
			d_cell_n="Number of Unique Statuses"
			pm_n="Number of enrollment months during year"
			male="Number of Male Beneficiaries"
			died_n="Number Dying in Year"
			mas_cash="MAS Cash Beneficiaries"
			mas_mn="MAS Medically Needy Beneficiaries"
			mas_pov="MAS Poverty-Related Beneficiaries"
			mas_oth="MAS Other Beneficiaries"
			mas_1115="MAS 1115 Exspansion Beneficiaries"
			boe_aged="BOE Aged Beneficiaries"
			boe_disabled="BOE Disabled Beneficiaries"
			boe_child="BOE Child Beneficiaries"
			boe_adult="BOE Adult Beneficiaries"
			boe_uchild="BOE Child (Unemployed Adult) Beneficiaries"
			boe_uadult="BOE Unemployed Adult Beneficiaries"
			boe_fchild="BOE Foster Child Beneficiaries"
			spending="Total Annual Spending across Beneficiaries"

			elg_months="Number of Person Months of Eligibility"
			_0="Number of Beneficiaries Age less than 1 year"
			_1_5="Number of Beneficiaries Age 1 to 5"
			_6_18="Number of Beneficiaries Age 6 to 18"
			_19_44="Number of Beneficiaries Age 19 to 44"
			_45_64="Number of Beneficiaries Age 45 to 64"
			_65_84="Number of Beneficiaries Age 65 to 84"
			_85p="Number of Beneficiaries Age 85 and above"
			mo_dual="Number of Person Months of Dual Eligibility"
			mo_mc="Number of Person Months of Managed Care Enrollment"
			mo_dsbl="Number of Person Months of Disability"
			mo_ltss="Number of Person Months of LTSS Use"
			mc_cat="Managed Care Category"
			dis_cat="Disability Category"
			dual_cat="Dual-Eligibility Category"
			ltss_cat="LTSS Use Category"
			foster_cat="Foster Care Category"
			;
		run;

/*

%macro cdps(indata,outdata);
	*3 get means of spending and CDPS and generate mult_c=(spend_c/cdps_c) (by 'chip','nmcd','','msg') (SQL);
	proc sql;
		create table spendavg as
		select age_servicetype, 
			AVG(TOT_MDCD_PYMT_AMT) as mspend_c,
			AVG(phys_clin_spending) AS m_physclinspend_c,
			AVG(FFS_PYMT_AMT_16) AS m_ffspymtspend_c,
			AVG(CDPS_SCORE) as cdps_c,
			AVG(TOT_MDCD_PYMT_AMT) / AVG(CDPS_SCORE) as mult_c,
			AVG(phys_clin_spending) AS m_physclinmult_c,
			AVG(FFS_PYMT_AMT_16) AS m_ffspymtmult_c
		from &indata.
		group by age_servicetype;
	quit;

	*4. join means to individual records by 'chip','nmcd','','msg'  and 
	   generate pspend_i=spend_i*mult_c and rspend_i=spend_i-pspend_i (SQL);
	proc sql;
		create table &outdata. as
		select T1.*,
			(T1.TOT_MDCD_PYMT_AMT) AS mspend_i,
			(T1.phys_clin_spending) AS m_physclinspend_i,
			(T1.FFS_PYMT_AMT_16) AS m_ffspymtspend_i,
			T2.mspend_c,
			T2.m_physclinspend_c,
			T2.m_ffspymtspend_c,
			T2.cdps_c,
			T2.mult_c,
			T2.m_physclinmult_c,
			T2.m_ffspymtmult_c,
			T1.CDPS_SCORE*T2.mult_c AS Pspend_i,
			(T1.TOT_MDCD_PYMT_AMT) - T1.CDPS_SCORE*T2.mult_c AS Rspend_i,
			(T1.phys_clin_spending) - T1.CDPS_SCORE*T2.m_physclinmult_c AS R_physclinspend_i,
			(T1.FFS_PYMT_AMT_16) - T1.CDPS_SCORE*T2.m_ffspymtmult_c AS R_ffspymtspend_i
		from &indata. T1 left join spendavg T2
		on T1.age_servicetype=T2.age_servicetype;
	quit;

%mend;
%cdps(indata=max_cdps_dia2l,outdata=indata_max);


/********************************************************
/*Initial processing to attach MSA and HRR info to files*
/********************************************************
%macro msa;
	proc sql;
		create table ahrf_msa_xwalk as
		select *,
			&year. as year,
			catx("-",state_cd,county_fips) as st_cnty,
			case when state_fips = '72' then 'PR' 
				else state_cd 
				end as state_cd_fx,
			case when metropolitanmicropolitanstatis = 'Metropolitan Statistical Area' then catx('-', state_cd, cbsacode)
				else catx('-', state_cd, "XXXXX")
				end as st_msa,
			case when missing(unemp_d) or missing(unemp_n) then 1 else 0 end as ahrf_msg
		from space.ahrf_msa;
		
		create table ahrf_aggre as
		select year, st_msa,
			1000*sum(hos_n)/sum(pop)label = "Number of hospital beds per 1k people, 2010" as beds, 
			1000*sum(md_n)/sum(pop)label = "Number of physicians per 1k people, 2010" as md, 
			sum(poverty_d)/sum(poverty_n) label = "Rate of persons in poverty" as povrate,
			sum(unemp_d)/sum(unemp_n) label = "Unemployment rate" as urate,
			sum(ahrf_msg) as sum_ahrf_msg
		from ahrf_msa_xwalk
		group by year, st_msa;
	quit;

	proc sql ;
		create table max_2012_msa_join as
		select  a.*, b.* /*,cbsatitle_fx as cbsatitle*
		from indata_max a left join ahrf_msa_xwalk (drop=year) b
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
		create table max_msa_2012 as
		select *
		from max_2012_msa_join
		where st_msa ne ' ';
	quit;
%mend;
%msa;

/***************************/
/*Collapse to specified var*/
/***************************/
			/*
Inpatient stays (TOT_IP_STAY_CNT)
Inpatient days (TOT_IP_DAY_CNT_STAYS)
Inpatient spending (medicaid) (FFS_PYMT_AMT_01)
Physician and clinic (combined) claims (FFS_CLM_CNT_08+FFS_CLM_CNT_12)phys_clin_claims
			phys_clin_spending
Physician and clinic (combined) spending (FFS_PYMT_AMT_08+FFS_PYMT_AMT_12)
Prescription drug claims(FFS_CLM_CNT_16)
Prescription drug spending (FFS_PYMT_AMT_16)
*

%macro collapse (indata,collapse_on,outdata);
	proc sql;
		create table &outdata. as
			select year, age_servicetype, age_cat, &collapse_on., dis_cat, dual_cat, foster_cat, ltss_cat, mc_cat,
			sum(TOT_MDCD_PYMT_AMT) as spending,
			sum(TOT_IP_STAY_CNT) as sum_TOT_IP_STAY_CNT,
			sum(TOT_IP_DAY_CNT_STAYS) as sum_TOT_IP_DAY_CNT_STAYS,
			sum(FFS_PYMT_AMT_01) as sum_FFS_PYMT_AMT_01,
			sum(phys_clin_claims) as sum_phys_clin_claims,
			sum(phys_clin_spending) as sum_phys_clin_spending,
			sum(FFS_CLM_CNT_16) as sum_FFS_CLM_CNT_16,
			sum(FFS_PYMT_AMT_16) as sum_FFS_PYMT_AMT_16,
			sum(mo_dual) as dual_mon, 
			sum(mo_mc) as mc_mon, 
			sum(mo_dsbl) as dis_mon, 
			sum(mo_ltss) as ltss_mon,
			sum(EL_ELGBLTY_MO_CNT) as elg_mon, 
			sum(cell_n) as cell_n,
			sum(d_servicetype_n) as d_servicetype_n, 
			sum(died_n) as died_n,
			sum(mas_cash) as mas_cash_n, 
			sum(mas_cash)/sum(cell_n) as mas_cash,
			sum(mas_mn) as mas_mn_n, 
			sum(mas_mn)/sum(cell_n) as mas_mn, 
			sum(mas_pov) as mas_pov_n, 
			sum(mas_pov)/sum(cell_n) as mas_pov, 
			sum(mas_1115) as mas_1115_n,
			sum(mas_1115)/sum(cell_n) as mas_1115,
			sum(mas_oth) as mas_oth_n, 
			sum(mas_oth)/sum(cell_n) as mas_oth, 
			sum(boe_aged) as boe_aged_n, 
			sum(boe_aged)/sum(cell_n) as boe_aged, 
			sum(boe_disabled) as boe_disabled_n, 
			sum(boe_disabled)/sum(cell_n) as boe_disabled, 
			sum(boe_child) as boe_child_n, 
			sum(boe_child)/sum(cell_n) as boe_child, 
			sum(boe_adult) as boe_adult_n, 
			sum(boe_adult)/sum(cell_n) as boe_adult, 
			sum(boe_uchild) as boe_uchild_n, 
			sum(boe_uchild)/sum(cell_n) as boe_uchild, 
			sum(boe_uadult) as boe_uadult_n, 
			sum(boe_uadult)/sum(cell_n) as boe_uadult, 
			sum(boe_fchild) as boe_fchild_n,
			sum(boe_fchild)/sum(cell_n) as boe_fchild,
			sum(male) as male_n, 
			sum(male)/sum(cell_n) as male, 
			sum(age_0) as _0_n, 
			sum(age_0)/sum(cell_n) as _0, 
			sum(age_1_5) as _1_5_n, 
			sum(age_1_5)/sum(cell_n) as _1_5, 
			sum(age_6_18) as _6_18_n, 
			sum(age_6_18)/sum(cell_n) as _6_18, 
			sum(age_19_44) as _19_44_n, 
			sum(age_19_44)/sum(cell_n) as _19_44, 
			sum(age_45_64) as _45_64_n, 
			sum(age_45_64)/sum(cell_n) as _45_64, 
			sum(age_65_84) as _65_84_n, 
			sum(age_65_84)/sum(cell_n) as _65_84, 
			sum(age_85p) as _85p_n, 
			sum(age_85p)/sum(cell_n) as _85p,
			mean(CDPS_SCORE) as cdps,
			mean(NOCDPS) as no_cdps_conds,
			mean(pspend_i) as pred_mcd_spd
			from &indata. 
			group by year, age_servicetype, age_cat, &collapse_on.,dis_cat, dual_cat, foster_cat, ltss_cat, mc_cat;
		quit;

%mend;
%collapse(indata=max_msa_2012,collapse_on=st_msa,outdata=msa_collapse);
%macro add_vars(collapsed_dat,collapsed_on,orig_dat,outdata);
	proc sql;
		/*join the beds, md, urate, and povrate to msa data*
		create table &outdata. as 
		select a.*, b.beds, b.md, b.urate, b.povrate
		from &collapsed_dat as a left join 
			(select distinct &collapsed_on., beds, md, urate, povrate from &orig_dat.) as b
		on a.&collapsed_on.=b.&collapsed_on.;
	quit;
%mend;

%add_vars(collapsed_dat=msa_collapse,collapsed_on=st_msa,orig_dat=ahrf_aggre, outdata=msa_arhfvars);

/*********************************
/*Get statistics and add to table*
/*********************************

%macro get_stats(indata=,indata_collapsed=,orig_data=,collapsevar=,outdata=);
	proc univariate data=&indata. noprint;
		class age_servicetype &collapsevar. ;
		var TOT_MDCD_PYMT_AMT Rspend_i phys_clin_spending R_physclinspend_i FFS_PYMT_AMT_16 R_ffspymtspend_i;
		output out=spend_pctls_&collapsevar.
		pctlpts = 10 25 50 75 90 95 99
		pctlpre=spd_p res_spd_p physclin_spd_p physclin_res_spd_p ffspymt_spd_p ffspymt_res_spd_p;
	run;

	proc univariate data=&indata. noprint;
		class &collapsevar. ;
		var TOT_MDCD_PYMT_AMT Rspend_i phys_clin_spending R_physclinspend_i FFS_PYMT_AMT_16 R_ffspymtspend_i;
		output out=spend_cap_&collapsevar.
		pctlpts =  99.5
		pctlpre=spd_p res_spd_p physclin_spd_p physclin_res_spd_p ffspymt_spd_p ffspymt_res_spd_p;
	run;

	/*put stats together*
	proc sql;
	  create table &indata._c AS
		select a.&collapsevar., a.age_servicetype, 
			a.TOT_MDCD_PYMT_AMT as mcd_spd, a.Rspend_i as res_mcd_spd, 
			((a.TOT_MDCD_PYMT_AMT>B.spd_p99_5)*B.spd_p99_5) as mcd_spd_TC,((A.Rspend_i>B.res_spd_p99_5)*B.spd_p99_5) as res_mcd_spd_TC,

			a.phys_clin_spending as physclin_spd, a.R_physclinspend_i as res_physclin_spd, 
			((a.phys_clin_spending>B.physclin_spd_p99_5)*B.physclin_spd_p99_5) as physclin_spd_TC,((A.R_physclinspend_i>B.physclin_res_spd_p99_5)*B.physclin_spd_p99_5) as res_physclin_spd_TC,

			a.FFS_PYMT_AMT_16 as ffspymt_spd, a.R_ffspymtspend_i as res_ffspymt_spd, 
			((a.FFS_PYMT_AMT_16>B.ffspymt_spd_p99_5)*B.ffspymt_spd_p99_5) as ffspymt_spd_TC,((A.R_ffspymtspend_i>B.ffspymt_res_spd_p99_5)*B.ffspymt_spd_p99_5) as res_ffspymt_spd_TC
		from &indata. a left join spend_cap_&collapsevar. b  
		on a.&collapsevar.=b.&collapsevar.;
	  quit;

	  /*get overall stats*
	proc univariate data=&indata._c noprint;
		class age_servicetype &collapsevar. ;
		var mcd_spd mcd_spd_TC res_mcd_spd res_mcd_spd_TC physclin_spd physclin_spd_TC res_physclin_spd res_physclin_spd_TC ffspymt_spd ffspymt_spd_TC res_ffspymt_spd res_ffspymt_spd_TC;
		output out=max_&collapsevar.
		sum=mcd_spd_tot mcd_spd_tot_TC res_mcd_spd_tot res_mcd_spd_tot_TC
			physclin_spd_tot physclin_spd_tot_TC physclin_res_spd_tot physclin_res_spd_tot_TC
			ffspymt_spd_tot ffspymt_spd_tot_TC ffspymt_res_spd_tot ffspymt_res_spd_tot_TC
		mean=spd_avg spd_avg_TC res_spd_avg res_spd_avg_TC
			physclin_spd_avg physclin_spd_avg_TC physclin_res_spd_avg physclin_res_spd_avg_TC
			ffspymt_spd_avg ffspymt_spd_avg_TC ffspymt_res_spd_avg ffspymt_res_spd_avg_TC
		stdmean=spd_se spd_se_TC res_spd_se res_spd_se_TC
			physclin_spd_se physclin_spd_se_TC physclin_res_spd_se physclin_res_spd_se_TC
			ffspymt_spd_se ffspymt_spd_se_TC ffspymt_res_spd_se ffspymt_res_spd_se_TC
		max=spd_max spd_TC_max res_spd_max res_spd_TC_max
			physclin_spd_max physclin_spd_TC_max physclin_res_spd_max physclin_res_spd_TC_max
			ffspymt_spd_max ffspymt_spd_TC_max ffspymt_res_spd_max ffspymt_res_spd_TC_max
		;
	run;

	/*put overall stats into final table*
	proc sql;
		create table fintab_&collapsevar._ac as
		select a.*,  
		B.mcd_spd_tot, 
		B.spd_avg, 
		B.spd_se, 
		B.spd_avg_tc, 
		B.spd_se_tc, 
		C.spd_p10, 
		C.spd_p25,
		C.spd_p50,
		C.spd_p75,
		C.spd_p90,
		C.spd_p95,
		C.spd_p99,
		B.res_mcd_spd_tot, 
		B.res_spd_avg, 
		B.res_spd_se, 
		B.res_spd_avg_tc, 
		B.res_spd_se_tc, 
		C.res_spd_p10, 
		C.res_spd_p25,
		C.res_spd_p50,
		C.res_spd_p75,
		C.res_spd_p90,
		C.res_spd_p95,
		C.res_spd_p99,
		B.res_spd_max,
		B.res_spd_tc_max, 
		B.spd_tc_max, 
		B.spd_max,

		B.physclin_spd_tot,
		B.physclin_spd_avg,
		B.physclin_spd_se,
		B.physclin_spd_tot_TC,
		B.physclin_spd_se_TC,
		C.physclin_spd_p10, 
		C.physclin_spd_p25,
		C.physclin_spd_p50,
		C.physclin_spd_p75,
		C.physclin_spd_p90,
		C.physclin_spd_p95,
		C.physclin_spd_p99,
		B.physclin_res_spd_tot, 
		B.physclin_res_spd_avg, 
		B.physclin_res_spd_se, 
		B.physclin_res_spd_avg_tc, 
		B.physclin_res_spd_se_tc, 
		C.physclin_res_spd_p10, 
		C.physclin_res_spd_p25,
		C.physclin_res_spd_p50,
		C.physclin_res_spd_p75,
		C.physclin_res_spd_p90,
		C.physclin_res_spd_p95,
		C.physclin_res_spd_p99,
		B.physclin_res_spd_max,
		B.physclin_res_spd_tc_max, 
		B.physclin_spd_tc_max, 
		B.physclin_spd_max,

		B.ffspymt_spd_tot,
		B.ffspymt_spd_avg,
		B.ffspymt_spd_se,
		B.ffspymt_spd_avg_TC,
		B.ffspymt_spd_se_TC,
		C.ffspymt_spd_p10, 
		C.ffspymt_spd_p25,
		C.ffspymt_spd_p50,
		C.ffspymt_spd_p75,
		C.ffspymt_spd_p90,
		C.ffspymt_spd_p95,
		C.ffspymt_spd_p99,
		B.ffspymt_res_spd_tot, 
		B.ffspymt_res_spd_avg, 
		B.ffspymt_res_spd_se, 
		B.ffspymt_res_spd_avg_tc, 
		B.ffspymt_res_spd_se_tc, 
		C.ffspymt_res_spd_p10, 
		C.ffspymt_res_spd_p25,
		C.ffspymt_res_spd_p50,
		C.ffspymt_res_spd_p75,
		C.ffspymt_res_spd_p90,
		C.ffspymt_res_spd_p95,
		C.ffspymt_res_spd_p99,
		B.ffspymt_res_spd_max,
		B.ffspymt_res_spd_tc_max, 
		B.ffspymt_spd_tc_max, 
		B.ffspymt_spd_max

		from &indata_collapsed. a 
			left join max_&collapsevar. b on a.age_servicetype=b.age_servicetype  and a.&collapsevar.=b.&collapsevar.
			left join spend_pctls_&collapsevar. c on a.age_servicetype=c.age_servicetype and a.&collapsevar.=c.&collapsevar.
			; 
	quit;

	%let maxvars = dual_mon mc_mon dis_mon ltss_mon elg_mon cell_n d_servicetype_n died_n mas_: boe_: male: _0: _1_5: _6_18: _19_44: _45_64: _65_84: _85p: cdps no_cdps_conds pred_mcd_spd mcd_spd_tot spd: res:;

	*mark too-small cells missing;
	data &outdata. ;
		set fintab_&collapsevar._ac;
		format 
			age_cat age.
			mc_cat mc.
			dis_cat dis.
			ltss_cat ltss.
			dual_cat dual.
			foster_cat foster.;
		label 	
			state_cd ="State Abbreviation"
			st_msa ="State-MSA Code"
			cbsatitle ="CBSA Name"
			age_servicetype ="Unique cell ID"
			age_cat ="Age Category"
			dual_cat ="Dual-Eligibility Category"
			mc_cat ="Managed Care Category"
			dis_cat ="Disability Category"
			ltss_cat ="LTSS Use Category"
			foster_cat ="Foster Care Category"
			dual_mon ="Number of Person Months of Dual Eligibility"
			mc_mon ="Number of Person Months of Managed Care Enrollment"
			dis_mon ="Number of Person Months of Disability"
			ltss_mon ="Number of Person Months of LTSS Use"
			elg_mon ="Number of Person Months of Eligibility"
			cell_n ="Number of Beneficiaries"
			d_servicetype_n ="Number of Unique Statuses"
			died_n ="Number Dying in Year"
			mas_cash_n ="MAS Cash Beneficiaries (N)"
			mas_cash ="MAS Cash Beneficiaries (%)"
			mas_mn_n ="MAS Medically Needy Beneficiaries (N)"
			mas_mn ="MAS Medically Needy Beneficiaries (%)"
			mas_pov_n ="MAS Poverty-Related Beneficiaries (N)"
			mas_pov ="MAS Poverty-Related Beneficiaries (%)"
			mas_oth_n ="MAS Other Beneficiaries (N)"
			mas_oth ="MAS Other Beneficiaries (%)"
			mas_1115_n ="MAS 1115 Exspansion Beneficiaries (N)"
			mas_1115 ="MAS 1115 Exspansion Beneficiaries (%)"
			boe_aged_n ="BOE Aged Beneficiaries (N)"
			boe_aged ="BOE Aged Beneficiaries (%)"
			boe_disabled_n ="BOE Disabled Beneficiaries (N)"
			boe_disabled ="BOE Disabled Beneficiaries (%)"
			boe_child_n ="BOE Child Beneficiaries (N)"
			boe_child ="BOE Child Beneficiaries (%)"
			boe_adult_n ="BOE Adult Beneficiaries (N)"
			boe_adult ="BOE Adult Beneficiaries (%)"
			boe_uchild_n ="BOE Child (Unemployed Adult) Beneficiaries (N)"
			boe_uchild ="BOE Child (Unemployed Adult) Beneficiaries (%)"
			boe_uadult_n ="BOE Unemployed Adult Beneficiaries (N)"
			boe_uadult ="BOE Unemployed Adult Beneficiaries (%)"
			boe_fchild_n ="BOE Foster Child Beneficiaries (N)"
			boe_fchild ="BOE Foster Child Beneficiaries (%)"
			male_n ="Number of Male Beneficiaries (N)"
			male ="Number of Male Beneficiaries (%)"
			_0_n ="Number of Beneficiaries Age less than 1 year (N)"
			_0 ="Number of Beneficiaries Age less than 1 year (%)"
			_1_5_n ="Number of Beneficiaries Age 1 to 5 (N)"
			_1_5 ="Number of Beneficiaries Age 1 to 5 (%)"
			_6_18_n ="Number of Beneficiaries Age 6 to 18 (N)"
			_6_18 ="Number of Beneficiaries Age 6 to 18 (%)"
			_19_44_n ="Number of Beneficiaries Age 19 to 44 (N)"
			_19_44 ="Number of Beneficiaries Age 19 to 44 (%)"
			_45_64_n ="Number of Beneficiaries Age 45 to 64 (N)"
			_45_64 ="Number of Beneficiaries Age 45 to 64 (%)"
			_65_84_n ="Number of Beneficiaries Age 65 to 84 (N)"
			_65_84 ="Number of Beneficiaries Age 65 to 84 (%)"
			_85p_n ="Number of Beneficiaries Age 85 and above (N)"
			_85p ="Number of Beneficiaries Age 85 and above (%)"
			cdps="Mean CDPS Score"
			no_cdps_conds="Proportion of beneficiaries with no CDPS diagnoses in year"
			pred_mcd_spd="Predicted Annual spending, from CDPS score"
			ahrf_msg ="Missing AHRF Data Flag"
			beds ="Number of hospital beds per 1k people, 2010"
			md ="Number of physicians per 1k people, 2010"
			urate ="Unemployment rate, 2012"
			povrate ="Rate of persons in poverty, 2012"

			mcd_spd_tot ="Total Annual Spending"
			spd_avg ="Mean Annual Spending per Enrollee"
			spending = "Annual Spending"
			sum_TOT_IP_STAY_CNT = "Annual Inpatient Stays"
			sum_TOT_IP_DAY_CNT_STAYS = "Annual Inpatient Days"
			sum_FFS_PYMT_AMT_01 = "Annual Inpatient Spending (Medicaid)"
			sum_phys_clin_claims = "Annual Physician and Clinic Claims"
			sum_FFS_CLM_CNT_16 = "Annual Prescription Drug Claims"
			spd_se ="Standard Error of Mean Annual Spending"
			spd_avg_tc ="Mean Annual Spending per Enrollee (Top Coded)"
			spd_se_tc ="Standard Error of Mean Annual Spending (Top Coded)"
			spd_tc_max ="Maximum Annual Spending per Enrollee (Top Coded)"
			spd_p10 ="10th Percentile of Annual Spending"
			spd_p25 ="25th Percentile of Annual Spending"
			spd_p50 ="50th Percentile of Annual Spending"
			spd_p75 ="75th Percentile of Annual Spending"
			spd_p90 ="90th Percentile of Annual Spending"
			spd_p95 ="95th Percentile of Annual Spending"
			spd_p99 ="99th Percentile of Annual Spending"
			spd_max ="Maximum Annual Spending per Enrollee"
			res_mcd_spd_tot ="Total Annual Spending Residual"
			res_spd_avg ="Mean Annual Spending Residual per Enrollee"
			res_spd_se ="Standard Error of Mean Annual Spending Residual"
			res_spd_avg_tc ="Mean Annual Spending Residual per Enrollee (Top Coded)"
			res_spd_se_tc ="Standard Error of Mean Annual Spending Residual (Top Coded)"
			res_spd_tc_max ="Maximum Annual Spending Residual per Enrollee (Top Coded)"
			res_spd_p10 ="10th Percentile of Annual Spending Residual"
			res_spd_p25 ="25th Percentile of Annual Spending Residual"
			res_spd_p50 ="50th Percentile of Annual Spending Residual"
			res_spd_p75 ="75th Percentile of Annual Spending Residual"
			res_spd_p90 ="90th Percentile of Annual Spending Residual"
			res_spd_p95 ="95th Percentile of Annual Spending Residual"
			res_spd_p99 ="99th Percentile of Annual Spending Residual"
			res_spd_max ="Maximum Annual Spending Residual per Enrollee"

			physclin_spd_tot ="Total Annual Physician and Clinic Spending"
			physclin_spd_avg = ="Mean Annual Physician and Clinic per Enrollee"
			sum_phys_clin_spending = "Annual Physician and Clinic Spending"
			physclin_spd_se ="Standard Error of Mean Annual Physician and Clinic Spending"
			physclin_spd_avg_tc ="Mean Annual Physician and Clinic Spending per Enrollee (Top Coded)"
			physclin_spd_se_tc ="Standard Error of Mean Annual Physician and Clinic Spending (Top Coded)"
			physclin_spd_tc_max ="Maximum Annual Physician and Clinic Spending per Enrollee (Top Coded)"
			physclin_spd_p10 ="10th Percentile of Annual Physician and Clinic Spending"
			physclin_spd_p25 ="25th Percentile of Annual Physician and Clinic Spending"
			physclin_spd_p50 ="50th Percentile of Annual Physician and Clinic Spending"
			physclin_spd_p75 ="75th Percentile of Annual Physician and Clinic Spending"
			physclin_spd_p90 ="90th Percentile of Annual Physician and Clinic Spending"
			physclin_spd_p95 ="95th Percentile of Annual Physician and Clinic Spending"
			physclin_spd_p99 ="99th Percentile of Annual Physician and Clinic Spending"
			physclin_spd_max ="Maximum Annual Physician and Clinic Spending per Enrollee"
			physclin_res_spd_tot ="Total Annual Physician and Clinic Spending Residual"
			physclin_res_spd_avg ="Mean Annual Physician and Clinic Spending Residual per Enrollee"
			physclin_res_spd_se ="Standard Error of Mean Annual Physician and Clinic Spending Residual"
			physclin_res_spd_avg_tc ="Mean Annual Physician and Clinic Spending Residual per Enrollee (Top Coded)"
			physclin_res_spd_se_tc ="Standard Error of Mean Annual Physician and Clinic Spending Residual (Top Coded)"
			physclin_res_spd_tc_max ="Maximum Annual Physician and Clinic Spending Residual per Enrollee (Top Coded)"
			physclin_res_spd_p10 ="10th Percentile of Annual Physician and Clinic Spending Residual"
			physclin_res_spd_p25 ="25th Percentile of Annual Physician and Clinic Spending Residual"
			physclin_res_spd_p50 ="50th Percentile of Annual Physician and Clinic Spending Residual"
			physclin_res_spd_p75 ="75th Percentile of Annual Physician and Clinic Spending Residual"
			physclin_res_spd_p90 ="90th Percentile of Annual Physician and Clinic Spending Residual"
			physclin_res_spd_p95 ="95th Percentile of Annual Physician and Clinic Spending Residual"
			physclin_res_spd_p99 ="99th Percentile of Annual Physician and Clinic Spending Residual"
			physclin_res_spd_max ="Maximum Annual Physician and Clinic Spending Residual per Enrollee"

			ffspymt_spd_tot ="Total Annual Prescription Drug Spending"
			ffspymt_spd_avg ="Mean Annual Prescription Drug Spending per Enrollee"
			sum_FFS_PYMT_AMT_16 = "Annual Prescription Drug Spending"
			ffspymt_spd_se ="Standard Error of Mean Annual Prescription Drug Spending"
			ffspymt_spd_avg_tc ="Mean Annual Prescription Drug Spending per Enrollee (Top Coded)"
			ffspymt_spd_se_tc ="Standard Error of Mean Annual Prescription Drug Spending (Top Coded)"
			ffspymt_spd_tc_max ="Maximum Annual Prescription Drug Spending per Enrollee (Top Coded)"
			ffspymt_spd_p10 ="10th Percentile of Annual Prescription Drug Spending"
			ffspymt_spd_p25 ="25th Percentile of Annual Prescription Drug Spending"
			ffspymt_spd_p50 ="50th Percentile of Annual Prescription Drug Spending"
			ffspymt_spd_p75 ="75th Percentile of Annual Prescription Drug Spending"
			ffspymt_spd_p90 ="90th Percentile of Annual Prescription Drug Spending"
			ffspymt_spd_p95 ="95th Percentile of Annual Prescription Drug Spending"
			ffspymt_spd_p99 ="99th Percentile of Annual Prescription Drug Spending"
			ffspymt_spd_max ="Maximum Annual Prescription Drug Spending per Enrollee"
			ffspymt_res_spd_tot ="Total Annual Prescription Drug Spending Residual"
			ffspymt_res_spd_avg ="Mean Annual Prescription Drug Spending Residual per Enrollee"
			ffspymt_res_spd_se ="Standard Error of Mean Annual Prescription Drug Spending Residual"
			ffspymt_res_spd_avg_tc ="Mean Annual Prescription Drug Spending Residual per Enrollee (Top Coded)"
			ffspymt_res_spd_se_tc ="Standard Error of Mean Annual Prescription Drug Spending Residual (Top Coded)"
			ffspymt_res_spd_tc_max ="Maximum Annual Prescription Drug Spending Residual per Enrollee (Top Coded)"
			ffspymt_res_spd_p10 ="10th Percentile of Annual Prescription Drug Spending Residual"
			ffspymt_res_spd_p25 ="25th Percentile of Annual Prescription Drug Spending Residual"
			ffspymt_res_spd_p50 ="50th Percentile of Annual Prescription Drug Spending Residual"
			ffspymt_res_spd_p75 ="75th Percentile of Annual Prescription Drug Spending Residual"
			ffspymt_res_spd_p90 ="90th Percentile of Annual Prescription Drug Spending Residual"
			ffspymt_res_spd_p95 ="95th Percentile of Annual Prescription Drug Spending Residual"
			ffspymt_res_spd_p99 ="99th Percentile of Annual Prescription Drug Spending Residual"
			ffspymt_res_spd_max ="Maximum Annual Prescription Drug Spending Residual per Enrollee"

			;
			
			array all_cells {*} &maxvars.; 
			if cell_n<11 then do;
				do i=1 to dim(all_cells);
					all_cells(i)=.S;
				end;
			end;
			drop i;
	run;

%mend;
%get_stats(indata=max_2012_msa_join,indata_collapsed=msa_arhfvars,orig_data=ahrf_msa_xwalk, collapsevar=st_msa,outdata=out.msa_dia2l_2012_&space_name.);


/**************************
/*Create summary workbooks*
/**************************
proc printto;run;
ods excel file="&report_folder.\output_dia2l_summarystats_&fname..xlsx";
ods excel options(sheet_name="msa" sheet_interval="none");
%let msa = out.msa_dia2l_2012_&space_name.;
/*%let hrr = &hrr_data.;*
data msa_num(keep=_NUMERIC_) msa_char(keep=_CHARACTER_);
    set &msa.;
run;
proc odstext;
	  p "&msa. descriptive statistics" / style=[color=red font_weight=bold];
run;
proc means stackodsoutput data=msa_num n mean std min max nmiss ;
	title "&msa. descriptive statistics for numeric variables";
run;
proc odstext;
	  p "Show cells marked .S in &msa." / style=[font_weight=bold];
run;
proc freq data=&msa;
	title "Show cells marked .S in &msa.";
	tables ffspymt_res_spd_se physclin_res_spd_se_TC/  missing;
	format ffspymt_res_spd_se physclin_res_spd_se_TC missing_S.;
run;

proc freq data=msa_char;
	title "&msa. descriptive statistics for character variables";
run;

ods excel close;

proc contents data= out.msa_dia2l_2012_&space_name.;run;
*/
