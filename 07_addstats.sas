/*******************************************************************************************************************/ 
/*	Purpose: Aggregate cleaned up MAX data from 01_studypop_analyticfile with CDPS data and AHRF and MSA/HRR data 
/*			on user-input geographic variable			
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)
/*	Notes: 
/*		1) Collapse macros for easier manipulation
/*******************************************************************************************************************/ 
/*Options to change*/
%macro prod();
	options obs=MAX;
	/*Log*/
	proc printto print="P:\MCD-SPVR\log\07_addstats_&sysdate..lst"
	               log="P:\MCD-SPVR\log\07_addstats_&sysdate..log" NEW;
	run;
	proc printto;run;
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
%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.));
%let space_name = %sysfunc(date(),date9.);
%let year = 2012;
%let dum_tab = %str(ods excel options(sheet_interval="table"); ods exclude all; data _null_; file print; put _all_; run; ods select all;);
%let report_folder = P:\MCD-SPVR\reports;
* AHRF HRR-state level file -- check for updated file;
%let ahrf_hrr = ahrf_hrr_state_v09_25_2018;

/*********************************/
/*Get statistics and add to table*/
/*********************************/
%let indata=space.temp_max_cdpsscores; /*from 05_addcdps*/
%let indata_collapsed=space.temp_msa_arhfvars_wageind;
%let orig_data=space.temp_ahrf_msa_xwalk;
%let collapsevar=st_msa;
%let outdata=out.msa_2012_02nov2019;

proc univariate data=&indata. noprint;
	class age_cell &collapsevar. ;
	var TOT_MDCD_PYMT_AMT Rspend_i;
	output out=spend_pctls_&collapsevar.
	pctlpts = 10 25 50 75 90 95 99
	pctlpre=spd_p res_spd_p;
run;

proc univariate data=&indata. noprint;
	class &collapsevar. ;
	var TOT_MDCD_PYMT_AMT Rspend_i;
	output out=spend_cap_&collapsevar.
	pctlpts =  99.5
	pctlpre=spd_p res_spd_p;
run;

proc print data=spend_cap_&collapsevar. (obs=10);run;

/*
proc sql;
	create table limit as
	select st_msa, spd_p99_5 , res_spd_p99_5
	from spend_cap_&collapsevar.;
quit;

proc  export data= limit
            outfile= "P:\MCD-SPVR\data\raw_data\tc_values.xlsx"
            dbms=xlsx replace;
run;
*/
/*put stats together*/
proc sql;
  create table max_cdpsscores_c AS
	select a.&collapsevar., a.age_cell, a.TOT_MDCD_PYMT_AMT as mcd_spd, a.Rspend_i as res_mcd_spd, 
		case when a.TOT_MDCD_PYMT_AMT>5000000 then 5000000 else TOT_MDCD_PYMT_AMT end as mcd_spd_TC,
		case when A.Rspend_i>5000000 then 5000000 else Rspend_i end as res_mcd_spd_TC
	from &indata. a left join spend_cap_&collapsevar. b  
	on a.&collapsevar.=b.&collapsevar.;
quit;

  /*get overall stats*/
proc univariate data=max_cdpsscores_c noprint;
	class age_cell &collapsevar. ;
	var mcd_spd mcd_spd_TC res_mcd_spd res_mcd_spd_TC;
	output out=max_&collapsevar.
	sum=mcd_spd_tot mcd_spd_tot_TC res_mcd_spd_tot res_mcd_spd_tot_TC
	mean=spd_avg spd_avg_TC res_spd_avg res_spd_avg_TC
	stdmean=spd_se spd_se_TC res_spd_se res_spd_se_TC
	max=spd_max spd_TC_max res_spd_max res_spd_TC_max
	;
run;

/*put overall stats into final table*/
proc sql;
	create table fintab_&collapsevar._ac as
	select a.*,  
	B.mcd_spd_tot,
	B.mcd_spd_tot_tc, 
	B.spd_avg,
	B.spd_avg_tc,  
	B.spd_se, 
	B.spd_se_tc, 
	C.spd_p10, 
	C.spd_p25,
	C.spd_p50,
	C.spd_p75,
	C.spd_p90,
	C.spd_p95,
	C.spd_p99,
	B.res_mcd_spd_tot,
	B.res_mcd_spd_tot_TC, 
	B.res_spd_avg,
	B.res_spd_avg_tc,  
	B.res_spd_se, 
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
	B.spd_max
	from &indata_collapsed. a 
		left join max_&collapsevar. b on a.age_cell=b.age_cell  and a.&collapsevar.=b.&collapsevar.
		left join spend_pctls_&collapsevar. c on a.age_cell=c.age_cell and a.&collapsevar.=c.&collapsevar.
		; 
quit;

%let maxvars = dual_mon mc_mon dis_mon ltss_mon elg_mon cell_n d_cell_n died_n mas_: boe_: male: _0: _1_5: _6_18: _19_44: _45_64: _65_84: _85p: cdps no_cdps_conds pred_mcd_spd mcd_spd_tot spd: res:;

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
	age_cell ="Unique cell ID"
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
	d_cell_n ="Number of Unique Statuses"
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
	spd_avg ="Mean Annual Spending"
	spd_se ="Standard Error of Mean Annual Spending"
	mcd_spd_tot_tc ="Total Spending (Top Coded)"
	spd_avg_tc ="Mean Annual Spending (Top Coded)"
	spd_se_tc ="Standard Error of Mean Annual Spending (Top Coded)"
	spd_tc_max ="Maximum Annual Spending (Top Coded)"
	spd_p10 ="10th Percentile of Annual Spending"
	spd_p25 ="25th Percentile of Annual Spending"
	spd_p50 ="50th Percentile of Annual Spending"
	spd_p75 ="75th Percentile of Annual Spending"
	spd_p90 ="90th Percentile of Annual Spending"
	spd_p95 ="95th Percentile of Annual Spending"
	spd_p99 ="99th Percentile of Annual Spending"
	spd_max ="Maximum Annual Spending"
	res_mcd_spd_tot ="Total Annual Spending Residual"
	res_spd_avg ="Mean Annual Spending Residual"
	res_spd_se ="Standard Error of Mean Annual Spending Residual"
	res_mcd_spd_tot_tc ="Total Annual Spending Residual (Top Coded)"
	res_spd_avg_tc ="Mean Annual Spending Residual (Top Coded)"
	res_spd_se_tc ="Standard Error of Mean Annual Spending Residual (Top Coded)"
	res_spd_tc_max ="Maximum Annual Spending Residual (Top Coded)"
	res_spd_p10 ="10th Percentile of Annual Spending Residual"
	res_spd_p25 ="25th Percentile of Annual Spending Residual"
	res_spd_p50 ="50th Percentile of Annual Spending Residual"
	res_spd_p75 ="75th Percentile of Annual Spending Residual"
	res_spd_p90 ="90th Percentile of Annual Spending Residual"
	res_spd_p95 ="95th Percentile of Annual Spending Residual"
	res_spd_p99 ="99th Percentile of Annual Spending Residual"
	res_spd_max ="Maximum Annual Spending Residual"
	wageind_WORK = "Medicare WORK Wage Index, 2012"
	wageind_PE = "Medicare PE Wage Index, 2012"
	wageind_MPE = "Medicare MPE Wage Index, 2012"
	partial_benf_mon = "Number of person months with partial benefits"
		;
		/*
		array all_cells {*} &maxvars.; 
		if cell_n<11 then do;
			do i=1 to dim(all_cells);
				all_cells(i)=.S;
			end;
		end;
		drop i;
		*/
run;

	proc printto;run;
