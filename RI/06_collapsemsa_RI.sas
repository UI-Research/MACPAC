/*******************************************************************************************************************/ 
/*	Purpose: Aggregate cleaned up MAX data from 01_studypop_analyticfile with CDPS data and AHRF and MSA/HRR data 
/*			on user-input geographic variable			
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)
/*	Notes: 
/*		1) Collapse macros for easier manipulation
/*******************************************************************************************************************/ 
/*Options to change*/

	options obs=MAX;
	/*Log*/
	proc printto;run;


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
%let indata=temp_max_cdpsscores; 
%let outdata= temp_msa_arhfvars_wageind;

%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.));
%let space_name = %sysfunc(date(),date9.);
%let report_folder = P:\MCD-SPVR\reports;
* AHRF HRR-state level file -- check for updated file;
%let ahrf_hrr = ahrf_hrr_state_v09_25_2018;

/*drop tables for space
proc sql;
	drop table &outdata.;
quit;
*/

/***************************/
/*Collapse to specified var*/
/***************************/

proc sql;
create table msa_collapse as
	select year, age_cell, age_cat, st_msa, dis_cat, dual_cat, foster_cat, ltss_cat, mc_cat,
		case when substr(st_msa,4) = "XXXXX" then "Non-metro area"
			else cbsatitle
		end as cbsatitle_fx, 
	sum(partial_benf_mon) as partial_benf_mon, 
	sum(TOT_MDCD_PYMT_AMT) as mcd_spd,
	sum(mo_dual) as dual_mon, 
	sum(mo_mc) as mc_mon, 
	sum(mo_dsbl) as dis_mon, 
	sum(mo_ltss) as ltss_mon,
	sum(EL_ELGBLTY_MO_CNT) as elg_mon, 
	sum(cell_n) as cell_n,
	sum(d_cell_n) as d_cell_n, 
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
	group by year, age_cell, age_cat, st_msa,dis_cat, dual_cat, foster_cat, ltss_cat, mc_cat, calculated cbsatitle_fx;
quit;
proc sql;
	select sum(cell_n)
	from msa_collapse;
	quit;
/*add MSA-aggregated vars from AHRF data*/
proc sql;
	/*join the beds, md, urate, and povrate to msa data*/
	create table msa_arhfvars as 
	select a.*, b.beds, b.md, b.urate, b.povrate
	from msa_collapse as a left join 
		(select distinct st_msa, beds, md, urate, povrate from space.temp_ahrf_aggre) as b
	on a.st_msa=b.st_msa;
quit;

/*add wage index data*/
/*recode medicare wage index MSAs to match our coding*/
proc sql;
	create table wageindex_2012 as
	select *,
	    case when MSA_NAME like '%NONMETROPOLITAN AREA' then catx("-",STATE,"XXXXX")
	    else catx("-",STATE,MSA_NO_)
	    end as st_msa
	from data.wageindex_2012
	where STATE ne "PR";
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
    create table space.temp_msa_arhfvars_wageind (drop = cbsatitle_fx) as
    select msa.*, cbsatitle_fx as cbsatitle,
        wag.WORK_fx as wageind_WORK, wag.PE_fx as wageind_PE, wag.MPE_fx as wageind_MPE
    from msa_arhfvars msa left join wageindex_2012_impu wag
    on msa.st_msa = wag.clms_st_msa;
quit;

proc sql;
	select sum(cell_n)
	from space.temp_msa_arhfvars_wageind 
	where substr(st_msa,1,2) ="RI";
quit;
	proc printto;run;
