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
	proc printto print="P:\MCD-SPVR\log\03_aggregate_&sysdate..lst"
	               log="P:\MCD-SPVR\log\03_aggregate_&sysdate..log" NEW;
	run;
%mend;

%macro test();	
	options obs=100000;
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
%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.));
%let space_name = %sysfunc(date(),date9.);
%let indata_max = space.id_pop_10oct2019; /*input data file from 01_studypop_analyticfile*/
%let year = 2012;
%let dum_tab = %str(ods excel options(sheet_interval="table"); ods exclude all; data _null_; file print; put _all_; run; ods select all;);
%let report_folder = P:\MCD-SPVR\reports;
* AHRF HRR-state level file -- check for updated file;
%let ahrf_hrr = ahrf_hrr_state_v09_25_2018;

/********************************/
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

/****************/
/* Add CDPS data*/
/****************/
%macro cdps;
	*1 concatenate state cdps files (DATA);
	data work.cdps_allst;
		set scores.cdps_asth:;
		state_cd=substr(recipno,1,2);
	run;
	
	*2 join CDPS to categories_full by RECIPNO (SQL) and drop benes with partial benefits;
	proc sql;
		create table cat_plus_cdps as
		select *
		from &indata_max. a left join cdps_allst (drop= male age state_cd) b 
		on a.recipno = b.RECIPNO
		where a.EL_RSTRCT_BNFT_FLG_LTST = "1";
	quit;

	proc sql;
		create table space.dropped_benes_wofullbenefits_&space_name. as
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

%mend;
%cdps;

/********************************************************/
/*Initial processing to attach MSA and HRR info to files*/
/********************************************************/
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
		select  a.*, b.* /*,cbsatitle_fx as cbsatitle*/
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

	data space.max_cdpsscores;
		set max_msa_2012;
	run;
%mend;
%msa;

/***************************/
/*Collapse to specified var*/
/***************************/
%macro collapse (indata,collapse_on,outdata);
	proc sql;
		create table &outdata. as
			select year, age_servicetype, age_cat, &collapse_on., dis_cat, dual_cat, foster_cat, ltss_cat, mc_cat,
			sum(sum(case when partial_benefits_1=1 then 1 else 0 end), 
				sum(case when partial_benefits_2=1 then 1 else 0 end), 
				sum(case when partial_benefits_3=1 then 1 else 0 end), 
				sum(case when partial_benefits_4=1 then 1 else 0 end), 
				sum(case when partial_benefits_5=1 then 1 else 0 end), 
				sum(case when partial_benefits_6=1 then 1 else 0 end), 
				sum(case when partial_benefits_7=1 then 1 else 0 end), 
				sum(case when partial_benefits_8=1 then 1 else 0 end), 
				sum(case when partial_benefits_9=1 then 1 else 0 end), 
				sum(case when partial_benefits_10=1 then 1 else 0 end), 
				sum(case when partial_benefits_11=1 then 1 else 0 end), 
				sum(case when partial_benefits_12=1 then 1 else 0 end)) as partial_benf_mon,
			sum(TOT_MDCD_PYMT_AMT) as spending,
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
%collapse(indata=space.max_cdpsscores,collapse_on=%str(st_msa),outdata=msa_collapse);

%macro add_vars(collapsed_dat,collapsed_on,orig_dat,outdata);
	proc sql;
		/*join the beds, md, urate, and povrate to msa data*/
		create table &outdata. as 
		select a.*, b.beds, b.md, b.urate, b.povrate
		from &collapsed_dat as a left join 
			(select distinct &collapsed_on., beds, md, urate, povrate from &orig_dat.) as b
		on a.&collapsed_on.=b.&collapsed_on.;
	quit;
%mend;

%add_vars(collapsed_dat=msa_collapse,collapsed_on=st_msa,orig_dat=ahrf_aggre, outdata=msa_arhfvars);

/******************************/
/*Add Medicare Wage Index data*/
/******************************/
/*recode medicare wage index MSAs to match our coding*/
proc sql;
	create table wageindex_2012 as
	select *, 
		case when MSA_NAME like '%NONMETROPOLITAN AREA' then catx("-",STATE,"XXXXX")
		else catx("-",STATE,MSA_NO_)
		end as st_msa
	from data.wageindex_2012
	where STATE ne "PR"; /*PR is not in the MAX data*/
quit;
/*add imputated data from Kyle Caswell*/
proc import datafile="P:\MCD-SPVR\data\raw_data\wageindmsa_v_clmsmsa_impu.csv"
     out=msa_xwalk
     dbms=csv
     replace;
     getnames=yes;
run;

proc sql;
	create table wageindex_2012_impu as
	select wag.*, wageind_st_msa, clms_st_msa, MSA_NO_, MSA_NAME, STATE,
			/*manual recodes from Kyle Caswell*/
		case when clms_st_msa = "WI-XXXXX" then 1.012 
		when clms_st_msa = "WY-XXXXX" then 1.000 
		else WORK 
		end as WORK_fx,
		case when clms_st_msa = "WI-XXXXX" then 0.967 
		when clms_st_msa = "WY-XXXXX" then 1.000 
		else PE 
		end as PE_fx,
		case when clms_st_msa = "WI-XXXXX" then 0.590
		when clms_st_msa = "WY-XXXXX" then 1.233 
		else MPE 
		end as MPE_fx
	from msa_xwalk xwalk left join wageindex_2012 (drop=VAR7) wag
	on xwalk.wageind_st_msa = wag.st_msa;
quit;

/*join to MAX data*/
proc sql;
	create table msa_arhfvars_wageind as
	select msa.*, 
		wag.WORK_fx as wageind_WORK, wag.PE_fx as wageind_PE, wag.MPE_fx as wageind_MPE
	from space.temp_msa_arhfvars msa left join wageindex_2012_impu wag
	on msa.st_msa = wag.clms_st_msa;
quit;

/*********************************/
/*Get statistics and add to table*/
/*********************************/
%macro get_stats(indata=,indata_collapsed=,orig_data=,collapsevar=,outdata=);
	proc univariate data=&indata. noprint;
		class age_servicetype &collapsevar. ;
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

	/*put stats together*/
	proc sql;
	  create table &indata._c AS
		select a.&collapsevar., a.age_servicetype, a.TOT_MDCD_PYMT_AMT as mcd_spd, a.Rspend_i as res_mcd_spd, 
			case when a.TOT_MDCD_PYMT_AMT>B.spd_p99_5 then B.spd_p99_5 else a.TOT_MDCD_PYMT_AMT end as mcd_spd_TC,
			case when A.Rspend_i>B.res_spd_p99_5 then B.res_spd_p99_5 else A.Rspend_i end as res_mcd_spd_TC
		from &indata. a left join spend_cap_&collapsevar. b  
		on a.&collapsevar.=b.&collapsevar.;
	  quit;

	  /*get overall stats*/
	proc univariate data=&indata._c noprint;
		class age_servicetype &collapsevar. ;
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
		B.spd_avg, 
		B.spd_se, 
		B.mcd_spd_tot_tc,
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
		B.res_mcd_spd_tot_tc, 
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
		B.spd_max
		from &indata_collapsed. a 
			left join max_&collapsevar. b on a.age_servicetype=b.age_servicetype  and a.&collapsevar.=b.&collapsevar.
			left join spend_pctls_&collapsevar. c on a.age_servicetype=c.age_servicetype and a.&collapsevar.=c.&collapsevar.
			; 
	quit;

	%let maxvars = dual_mon mc_mon dis_mon ltss_mon elg_mon cell_n d_servicetype_n died_n mas_: boe_: male: _0: _1_5: _6_18: _19_44: _45_64: _65_84: _85p: cdps no_cdps_conds pred_mcd_spd mcd_spd_tot spd: res:;

	*mark too-small cells missing -skipping this for now; 
	data &outdata. ;
		set fintab_&collapsevar._ac (drop = spending);
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

%mend;
%get_stats(indata=space.max_cdpsscores,indata_collapsed=msa_arhfvars_wageind,orig_data=space.temp_ahrf_msa_xwalk, collapsevar=st_msa,outdata=out.msa_2012_&space_name.);

ods excel file="&report_folder.\output_summarystats_&fname..xlsx";
ods excel options(sheet_name="msa" sheet_interval="none");
%let msa = space.msa_2012_11oct2019;

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
	tables spd_p50 res_spd_max/  missing;
	format spd_p50 res_spd_max missing_S.;
run;

proc freq data=msa_char;
	title "&msa. descriptive statistics for character variables";
run;

&dum_tab.;
ods excel options(sheet_name="Contents" sheet_interval="none");
proc contents data=&msa.;run;

ods excel close;
/*export a stata copy;
proc export data=out.msa_allcells_&date.
  outfile= "P:\MCD-SPVR\data\workspace\output\msa_allcells_&date..dta" replace;
run;

proc export data=out.msa_nosmallcells_&date.
  outfile= "P:\MCD-SPVR\data\workspace\output\msa_nosmallcells_&date..dta" replace;
run;
*/
proc printto;run;
