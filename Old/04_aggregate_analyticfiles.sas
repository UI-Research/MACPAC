/*******************************************************************************************************************/ 
/*	Purpose: Aggregate cleaned up MAX data from 01_studypop_analyticfile with CDPS data and AHRF and MSA data 
/*			on user-input geographic variable			
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)
/*	Notes: 
/*		1) Collapse macros for easier manipulation
/*******************************************************************************************************************/ 

* Date for version control;
%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.));
%let space_name = %sysfunc(date(),date9.);

%let indata_max = space.id_pop_07dec2018;
options obs=500000;
* log;
*PROC PRINTTO PRINT="P:\MCD-SPVR\log\02_max_ahrf_&sysdate..lst"
               LOG="P:\MCD-SPVR\log\02_max_ahrf_&sysdate..log" NEW;
*RUN;

libname data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;

/********************************************************************************/
/* Need to join MSA data (space.ahrf_msa) and HRR data (proc import below) and 
/* spending data (space.person_spend_112818)
/* Apply labels
/* Mask low counts
/* Get summary stats
/********************************************************************************/
/*
PROC IMPORT OUT= hrr_zip DATAFILE="&hrr_zip."
            DBMS=xls REPLACE;
RUN;
*/

proc format library=library;
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
quit;

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
	create table ahrf_msa_xwalk as
	select *, 2012 as year, 
		case
			when state_fips = '72' then 'PR'
			else state_cd
		end as state_cd_fx,
		case 
			when metropolitanmicropolitanstatis = 'Metropolitan Statistical Area' then catx('-', state_cd, cbsacode)
			else catx('-', state_cd, "XXXXX")
		end as st_msa,
		case 
			when metropolitanmicropolitanstatis = 'Metropolitan Statistical Area' then "Non-Metro-Rest-of-State"
			else cbsatitle
		end as cbsatitle_fx
	from space.ahrf_msa;
quit;

proc sql;
	create table ahrf_msa_2012 as
	select state_cd_fx as state_cd, st_msa, cbsatitle_fx as cbsatitle,
		catx('-', state_cd_fx,county_fips) as st_cnty,
		1000*sum(hos_n)/sum(pop) as beds, 
		1000*sum(md_n)/sum(pop) as md, 
		sum(poverty_d)/sum(poverty_n) as povrate,
		sum(unemp_d)/sum(unemp_n) as urate, 
		0 as _ahrf_msg
	from ahrf_msa_xwalk (drop = cbsatitle state_cd)
	group by year, state_cd_fx, st_msa, cbsatitle_fx;
quit;

proc sql ;
	create table max_msa_2012 as
	select  a.*, b.st_msa, b.cbsatitle
	from &indata_max. a left join ahrf_msa_2012 b
	ON A.cnty_fx=B.st_cnty;
quit;

proc freq data=max_msa_2012;
	tables st_msa cbsatitle cnty_fx/list missing;
	format st_msa cbsatitle cnty_fx $missing_char.;
run;

proc sql;
	title Obs without ST_MSA matches;
	select st_cnty as county,count(st_cnty) as number_missing
	from max_msa_2012
	where st_msa = ' '
	group by st_cnty;
quit;

proc sql;
	create table max_msapop_2012 as
		select year, state_cd as state, st_msa , cbsatitle, cell_age, age_cat, dual_cat, mc_cat, dis_cat, ltss_cat, foster_cat, 
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
		sum(age_85p)/sum(cell_n) as _85p 
		from max_msa_2012
		GROUP BY year, cell_age, state_cd, st_msa , cbsatitle, age_cat, cell, mc_cat, dis_cat, dual_cat, ltss_cat, foster_cat;
	quit;

	
proc univariate data=max_msa_2012 noprint;
	class cell_age  st_msa ;
	var TOT_MDCD_PYMT_AMT;
	output out=spend_pctls
	pctlpts = 10 25 50 75 90 95 99
	pctlpre=spd_p;
run;

proc univariate data=max_msa_2012 noprint;
	class  st_msa ;
	var TOT_MDCD_PYMT_AMT;
	output out=spend_cap
	pctlpts =  99.5
	pctlpre=p;
run;

proc sql;
  create table max_msa_2012_c AS
	select A.st_msa, A.cell_age, A.TOT_MDCD_PYMT_AMT as mcd_spd, ((A.TOT_MDCD_PYMT_AMT>B.p99_5)*B.p99_5) as mcd_spd_TC 
	FROM max_msa_2012 A 
		LEFT JOIN spend_cap B ON  A.st_msa=B.st_msa;
  quit;
run;


proc univariate data=max_msa_2012_c noprint;
	class cell_age st_msa ;
	var mcd_spd mcd_spd_TC;
	output out=max_msaspend_2012
	sum=mcd_spd_tot mcd_spd_tot_TC 
	mean=spd_avg spd_avg_TC
	stdmean=spd_se spd_se_TC
	max=spd_max spd_TC_max
	;
run;


proc sql;
	create table max_msafull_2012 as
	select A.*, 
	D._ahrf_msg,
	D.beds, 
	D.md, 
	D.urate, 
	D.povrate, 
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
	B.spd_tc_max, 
	B.spd_max
	from max_msapop_2012 A 
		LEFT JOIN ahrf_msa_2012 D ON  A.st_msa=D.st_msa
		LEFT JOIN max_msaspend_2012 B ON A.cell_age=B.cell_age  AND A.st_msa=B.st_msa
		LEFT JOIN spend_pctls C ON A.cell_age=C.cell_age AND  A.st_msa=C.st_msa
		; 
	quit;


data max_msafull_2012;
set max_msafull_2012 ;
label 	
state ="State Abbreviation"
st_msa ="State-MSA Code"
cbsatitle ="CBSA Name"
cell_age ="Unique cell ID"
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
ahrf_msg ="Missing AHRF Data Flag"
beds ="Number of hospital beds per 1k people, 2010"
md ="Number of physicians per 1k people, 2010"
urate ="Unemployment rate, 2012"
povrate ="Rate of persons in poverty, 2012"
mcd_spd_tot ="Total Annual Spending"
spd_avg ="Mean Annual Spending per Enrollee"
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
	;
	if missing(urate) then ahrf_msg=1; else ahrf_msg=0;

	format age_cat age.
	   mc_cat mc.
	   dis_cat dis.
	   ltss_cat ltss.
	   dual_cat dual.
	   foster_cat foster.;

run;
proc sort data=max_msafull_2012 out=max_msafull_2012;
by year state st_msa  cell_age;
run;

*drop observations missing category data;
data msa_allcells ;
	set max_msafull_2012;
	where ~missing(year) and ~missing(age_cat) and ~missing(cell_age) and ~missing(dual_cat) and ~missing(mc_cat) and ~missing(dis_cat) and ~missing(ltss_cat) and ~missing(st_msa);
run;

%let maxvars = dual_mon mc_mon dis_mon ltss_mon elg_mon cell_n d_cell_n died_n mas_: boe_: male: _0: _1_5: _6_18: _19_44: _45_64: _65_84: _85p: mcd_spd_tot spd:;

*mark too-small cells missing;
data msa_nosmallcells ;
	set msa_allcells;
  array max {*} &maxvars.; /*change array name to non-reserved word*/
  if cell_n<11 then do;
    do i=1 to dim(max);
      max(i)=.S;
    end;
  end;
  drop i;
run;

/*

*export a stata copy;
proc export data=out.msa_allcells_&date.
  outfile= "P:\MCD-SPVR\data\workspace\output\msa_allcells_&date..dta" replace;
run;

proc export data=out.msa_nosmallcells_&date.
  outfile= "P:\MCD-SPVR\data\workspace\output\msa_nosmallcells_&date..dta" replace;
run;

*/
/***************************************************/
/*03
/***************************************************/

*collapse to zip-cell level;
proc sql;
	 create table max_zip_2012 as
      select cell, age_cat, state_cd, zip_code, 
        sum(cell_n) as cell_n, 
        sum(d_cell_n) as d_cell_n, 
        sum(male) as male, 
        sum(died_n) as died_n,
		sum(mas_cash) as mas_cash,
        sum(mas_mn) as mas_mn, 
        sum(mas_pov) as mas_pov, 
        sum(mas_oth) as mas_oth, 
        sum(mas_1115) as mas_1115,
		sum(boe_aged) as boe_aged, 
        sum(boe_disabled) as boe_disabled, 
        sum(boe_child) as boe_child, 
        sum(boe_adult) as boe_adult, 
		sum(boe_uchild) as boe_uchild, 
        sum(boe_uadult) as boe_uadult, 
        sum(boe_fchild) as boe_fchild, 
        sum(TOT_MDCD_PYMT_AMT) as spending,
		sum(age_0) as _0, 
        sum(age_1_5) as _1_5, 
        sum(age_6_18) as _6_18, 
        sum(age_19_44) as _19_44, 
        sum(age_45_64) as _45_64, 
        sum(age_65_84) as _65_84, 
        sum(age_85p) as _85p, 
		sum(EL_ELGBLTY_MO_CNT) as elg_months, 
        sum(mo_dual) as mo_dual, 
        sum(mo_mc) as mo_mc, 
        sum(mo_dsbl) as mo_dsbl, 
        sum(mo_ltss) as mo_ltss

		from max_zip_complete
      group by cell, age_cat, state_cd, zip_code;
quit;

*collapse foster care, reorder vars;
proc sql;
	 create table out.Max_zip_&date. as
      select 
        cell, 
		state_cd,
        zip_code,
        age_cat, 
        mc_cat, 
        dis_cat,
        ltss_cat,
        dual_cat, 
        foster_cat, 
        sum(cell_n) as cell_n, 
        sum(d_cell_n) as d_cell_n, 
		sum(male) as male, 
        sum(died_n) as died_n,
		sum(mas_cash) as mas_cash,
        sum(mas_mn) as mas_mn, 
        sum(mas_pov) as mas_pov, 
        sum(mas_oth) as mas_oth, 
        sum(mas_1115) as mas_1115,
		sum(boe_aged) as boe_aged, 
        sum(boe_disabled) as boe_disabled, 
        sum(boe_child) as boe_child, 
        sum(boe_adult) as boe_adult, 
		sum(boe_uchild) as boe_uchild, 
        sum(boe_uadult) as boe_uadult, 
        sum(boe_fchild) as boe_fchild, 

		sum(_0) as _0,
        sum(_1_5) as _1_5, 
        sum(_6_18) as _6_18, 
        sum(_19_44) as _19_44, 
        sum(_45_64) as _45_64, 
        sum(_65_84) as _65_84, 
        sum(_85p) as _85p,
         
		sum(elg_months) as elg_months, 
        sum(mo_dual) as mo_dual, 
        sum(mo_mc) as mo_mc, 
        sum(mo_dsbl) as mo_dsbl, 
        sum(mo_ltss) as mo_ltss,

        sum(spending) as spending

		from out.Max_zip_&date.
      group by year, cell_age, state_cd, zip_code
quit;

*label;
data out.Max_zip_&date.;
	set out.Max_zip_&date.;
	where not missing(zip_code);
	format cell_n d_cell_n male died_n _: boe: mas: poverty: unemp: elg_months mo: COMMA16.;
	format spending DOLLAR16.;

			label 	state_cd="State Name Abbreviation"
			zip="Zip Code"
			cell="Beneficiary Cell Type"
			cell_age="Beneficiary Cell Type"
			age_cat="Age Category"
			cell_n="Number of Beneficiaries"
			d_cell_n="Number of Unique Statuses"
			elg_months="Number of Person Months of Eligibility"
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
			spending="Total Annual Spending"
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
			ahrf_msg="Missing AHRF Data Flag"
			;
run;
*export stata copy of;
proc export data=out.Max_zip_&date.
  outfile= "P:\MCD-SPVR\data\workspace\output\Max_zip_&date..dta";
run;
*collapse to zip-cell level;
proc sql;
	 create table max_zip_2012 as
      select cell, age_cat, state_cd, zip_code,
	    
        sum(cell_n) as cell_n, 
        sum(d_cell_n) as d_cell_n, 
        sum(male) as male, 
        sum(died_n) as died_n,
		sum(mas_cash) as mas_cash,
        sum(mas_mn) as mas_mn, 
        sum(mas_pov) as mas_pov, 
        sum(mas_oth) as mas_oth, 
        sum(mas_1115) as mas_1115,
		sum(boe_aged) as boe_aged, 
        sum(boe_disabled) as boe_disabled, 
        sum(boe_child) as boe_child, 
        sum(boe_adult) as boe_adult, 
		sum(boe_uchild) as boe_uchild, 
        sum(boe_uadult) as boe_uadult, 
        sum(boe_fchild) as boe_fchild, 
        sum(TOT_MDCD_PYMT_AMT) as spending,
		sum(age_0) as _0, 
        sum(age_1_5) as _1_5, 
        sum(age_6_18) as _6_18, 
        sum(age_19_44) as _19_44, 
        sum(age_45_64) as _45_64, 
        sum(age_65_84) as _65_84, 
        sum(age_85p) as _85p, 
		sum(EL_ELGBLTY_MO_CNT) as elg_months, 
        sum(mo_dual) as mo_dual, 
        sum(mo_mc) as mo_mc, 
        sum(mo_dsbl) as mo_dsbl, 
        sum(mo_ltss) as mo_ltss

		from max_zip_complete
      group by cell, age_cat, state_cd, zip_code;
quit;

*add cat variables and formatting;
data out.max_zip_&date. ;
	set max_zip_2012 ;
	where not missing(cell) and not missing(age_cat);

	if cell < 9 then dual_cat=0;
	else dual_cat=1;
	if cell in(1, 2, 3, 4, 9, 10, 11, 12) then mc_cat=1;
	else mc_cat=0;
	if cell in(1, 2, 5, 6, 9, 10, 13, 14) then dis_cat=1;
	else dis_cat=0;
	if cell in(1, 3, 5, 7, 9, 11, 13, 15) then ltss_cat=1;
	else ltss_cat=0;
	if cell=17 then do;
		age_cat=1;
		mc_cat=9;
		dis_cat=9;
		ltss_cat=9;
		dual_cat=9;
		foster_cat=1;
	end;
	else foster_cat=0;

	year=2012;

   *create single cell var;
   *if not missing(cell) then do;
   if not missing(cell) & not missing(age_cat) then do;
	   if age_cat=1 then cell=catx('_','child',cell);
	   else if age_cat=2 then cell=catx('_','adult',cell);
	   else if age_cat=3 then cell=catx('_','senior',cell);
	end;

	rename cell_age=cell;

	format age_cat age.
	   mc_cat mc.
	   dis_cat dis.
	   ltss_cat ltss.
	   dual_cat dual.
	   foster_cat foster.;

run;

*collapse foster care, reorder vars;
proc sql;
	 create table out.Max_zip_&date. as
      select 
        cell, 
		state_cd,
        zip_code,
        age_cat, 
        mc_cat, 
        dis_cat,
        ltss_cat,
        dual_cat, 
        foster_cat, 
        sum(cell_n) as cell_n, 
        sum(d_cell_n) as d_cell_n, 
		sum(male) as male, 
        sum(died_n) as died_n,
		sum(mas_cash) as mas_cash,
        sum(mas_mn) as mas_mn, 
        sum(mas_pov) as mas_pov, 
        sum(mas_oth) as mas_oth, 
        sum(mas_1115) as mas_1115,
		sum(boe_aged) as boe_aged, 
        sum(boe_disabled) as boe_disabled, 
        sum(boe_child) as boe_child, 
        sum(boe_adult) as boe_adult, 
		sum(boe_uchild) as boe_uchild, 
        sum(boe_uadult) as boe_uadult, 
        sum(boe_fchild) as boe_fchild, 

		sum(_0) as _0,
        sum(_1_5) as _1_5, 
        sum(_6_18) as _6_18, 
        sum(_19_44) as _19_44, 
        sum(_45_64) as _45_64, 
        sum(_65_84) as _65_84, 
        sum(_85p) as _85p,
         
		sum(elg_months) as elg_months, 
        sum(mo_dual) as mo_dual, 
        sum(mo_mc) as mo_mc, 
        sum(mo_dsbl) as mo_dsbl, 
        sum(mo_ltss) as mo_ltss,

        sum(spending) as spending

		from out.Max_zip_&date.
      group by cell, age_cat, mc_cat, dis_cat, ltss_cat, dual_cat, foster_cat, state_cd, zip_code;
quit;

*label;
data out.Max_zip_&date.;
	set out.Max_zip_&date.;
	where not missing(zip_code);
	format cell_n d_cell_n male died_n _: boe: mas: poverty: unemp: elg_months mo: COMMA16.;
	format spending DOLLAR16.;

			label 	state_cd="State Name Abbreviation"
			zip="Zip Code"
			cell="Beneficiary Cell Type"
			cell_age="Beneficiary Cell Type"
			age_cat="Age Category"
			cell_n="Number of Beneficiaries"
			d_cell_n="Number of Unique Statuses"
			elg_months="Number of Person Months of Eligibility"
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
			spending="Total Annual Spending"
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
			ahrf_msg="Missing AHRF Data Flag"
			;
run;
*export stata copy of;
proc export data=out.Max_zip_&date.
  outfile= "P:\MCD-SPVR\data\workspace\output\Max_zip_&date..dta";
run;


******************************;
* Create HRR-state level file ;
******************************;

* HRR-ZIP mapping file to merge wtih MAX data;
PROC IMPORT OUT= hrr_zip DATAFILE="&hrr_zip."
            DBMS=xls REPLACE;
RUN;
data hrr_zip;
  set hrr_zip;
  
  format tmp1 $ 5. hrrnum_c $ 3.;
  length hrrname $27.;

  * rename zip code for merge;
  zip_code=zipcode12;
  tmp1 = put(hrrnum,5.);
  tmp1=compress(tmp1);
  len=length(tmp1);

  * add leading zeros;
  if len=2 then tmp1=cat('0',tmp1);
  else if len=1 then tmp1=cat('00',tmp1);
  tmp1=compress(tmp1);
  hrrnum_c=tmp1;

  * add HRR name = HRR City & HRR state;
  hrrname=cat(trim(hrrcity),'-',trim(hrrstate));
  len2=length(hrrname);
  
  keep zip_code hrrnum hrrnum_c hrrname hrrstate;
run;
proc sort data=hrr_zip;
  by zip_code;
run;
* Attach HRR IDs to individual-level data file;
proc sort data=max_zip_complete;
  by zip_code;
run;
* use file "MAX_ZIP_COMPLETE" (individual-level file) for HRR-state level calculations --> avg, percentiles, etc.;
* merge with HRR-Zip code mapping file;
* redefine "cell" to account for age groups;
* many to one merge;
data max_zip_hrr;
  merge max_zip_complete (in=a where=(zip_code~=.) keep=BENE_ID zip_code state_cd cell age age_cat TOT_MDCD_PYMT_AMT )
        hrr_zip          (in=b);
  by zip_code;
  
  * combine info on cell & age category -- e.g.,  cell = adult_01
    class statement with proc means only allows 2 variables;
  format cell $ 9.;
  if not missing(cell) & not missing(age_cat) then do;
    if      age_cat=1 then cell=catx('_','child',cell);
    else if age_cat=2 then cell=catx('_','adult',cell);
    else if age_cat=3 then cell=catx('_','senior',cell);
  end;
  cell=compress(cell);
  
  * keep only records in individual-level data;
  if       a & ~b then _zip_hrr=1;
  else if ~a &  b then _zip_hrr=2;
  else if  a &  b then _zip_hrr=3;
  if a;
run;
* check file -- collapse variables;
proc freq data=max_zip_hrr;
  table _zip_hrr cell age_cat state_cd/missing; 
run;
* Note: there missing cell values & age category values. This needs to be addressed before collapse;
proc freq data=max_zip_hrr;
  table age*cell/missing norow nocol nopercent; 
run;
proc freq data=max_zip_hrr;
  where _zip_hrr=3 & not missing(cell) & not missing(age_cat);
  table _zip_hrr cell age_cat state_cd/missing;
run;
* Drop those with missing info on collapse variables & create HRR-state ID & records only in HRR-ZIP mapping file;
data max_zip_hrr_v2;
  set max_zip_hrr ( where=( _zip_hrr=3 & not missing(cell) & not missing(age_cat) & not missing(state_cd) ) );
  * Unique HRR-state ID. three character HRR ID - State abbreviation [XXX]-[XX];
  format hrr_state $ 6.;
  hrr_state=cat(hrrnum_c,'-',state_cd);
run;
proc freq data=max_zip_hrr_v2;
  table hrr_state state_cd/missing;
run;
* 99.5th Percentile of *** HRR-State Unit ***  for top coding;
proc univariate data=max_zip_hrr_v2 noprint;
  class hrr_state;
  var TOT_MDCD_PYMT_AMT;
  output out=hrr_state_p995 pctlpts =99.5 pctlpre=mcd_spd_ pctlname=p995;
run;
* merge 99.5th percentile of HRR-State to individual-level file for top coded spending;
proc sort data=max_zip_hrr_v2;
  by hrr_state;
run;
data max_zip_hrr_v3;
  merge max_zip_hrr_v2
        hrr_state_p995 (keep=hrr_state mcd_spd_p995);
  by hrr_state;
  * rename spending var;
  mcd_spd=TOT_MDCD_PYMT_AMT;
  * top coded version of spending var;
  mcd_spd_TC=TOT_MDCD_PYMT_AMT;
  if TOT_MDCD_PYMT_AMT>mcd_spd_p995 then mcd_spd_TC=mcd_spd_p995;
  * flag for top coded obs;
  _mcd_spd_p995=0;
  if TOT_MDCD_PYMT_AMT>mcd_spd_p995 then _mcd_spd_p995=1;
run;
* Percentiles by HRR-state-cell-age group;
proc univariate data=max_zip_hrr_v2 noprint;
  class hrr_state cell;
  var TOT_MDCD_PYMT_AMT;

  * output file name, percentiles, prefix of column variable namme;
  output out=hrr_state_pctiles
  pctlpts = 10 25 50 75 90 95 99 
  pctlpre=mcd_spd_
  pctlname=p10 p25 p50 p75 p90 p95 p99;
  /* cipctldf=(lowerpre=lb_p upperpre=ub_p); Confidence intervals for percentiles p95 --> unexpected results */
run;
* Average spending & top coded version of avg spending, standard errors & max at HRR-state-cell level;
proc univariate data=max_zip_hrr_v3 noprint;
  class hrr_state cell;
  var mcd_spd mcd_spd_TC;
  * mean, max & top coded mean;
  output out=hrr_state_means 
  mean=mcd_spd_bar mcd_spd_TC_bar 
  STDMEAN=mcd_spd_se mcd_spd_TC_se 
  max=mcd_spd_max;
run;
* All stats in one file;
proc sort data=max_zip_hrr_v2;
  by hrr_state cell age_cat;
run;
data hrr_state_stats;
  merge hrr_state_means
        hrr_state_pctiles;
  
  by hrr_state cell;

  label mcd_spd_bar    = "Average Medicaid spending per enrollee"
        mcd_spd_se     = "Standard error, average spending per enrollee"
        mcd_spd_TC_bar = "Average Medicad spending per enrollee, top coded p99.5"
        mcd_spd_TC_se  = "Standard error, average spending per enrollee top coded"
        mcd_spd_max    = "MAX, total spending"

        mcd_spd_p10    = "10th percentile, total spending"
        mcd_spd_p25    = "25th percentile, total spending"
        mcd_spd_p50    = "50th percentile, total spending"
        mcd_spd_p75    = "75th percentile, total spending"
        mcd_spd_p90    = "90th percentile, total spending"
        mcd_spd_p95    = "95th percentile, total spending"
        mcd_spd_p99    = "99th percentile, total spending";
        /*
        lb_p10 = "Lower bound, 95% CI, p10"
        lb_p25 = "Lower bound, 95% CI, p25"
        lb_p50 = "Lower bound, 95% CI, p50"
        lb_p75 = "Lower bound, 95% CI, p75"
        lb_p90 = "Lower bound, 95% CI, p90"
        lb_p95 = "Lower bound, 95% CI, p95"
        lb_p99 = "Lower bound, 95% CI, p99"
        ub_p10 = "Upper bound, 95% CI, p10"
        ub_p25 = "Upper bound, 95% CI, p25"
        ub_p50 = "Upper bound, 95% CI, p50"
        ub_p75 = "Upper bound, 95% CI, p75"
        ub_p90 = "Upper bound, 95% CI, p90"
        ub_p95 = "Upper bound, 95% CI, p95"
        ub_p99 = "Upper bound, 95% CI, p99";
		*/
run;
* merge 99.5th percentile -- many to one merge;
data hrr_state_stats_v2;
  merge hrr_state_stats (in=a)
        hrr_state_p995  (in=b keep=hrr_state mcd_spd_p995 );
  by hrr_state;
run;
* crete HRR-state level file from zip code cell level file;
proc sort data=out.Max_zip_&date.
               out=Max_zip;
  by zip_code;
run;
proc freq data=Max_zip;
  table cell state_cd age_cat/missing;
run;
* Many to one merge - final file at ZIP-cell-age group level; 
data zip_hrr_state;
  merge max_zip (in=a)
        hrr_zip (in=b);
  by zip_code;
  format hrr_state $ 6.;
  
  * unique HRR-state ID;
  hrr_state=cat(hrrnum_c,'-',state_cd);
  
  * Merge key;
  if       a & ~b then _merge_zip_hrr=1;
  else if ~a &  b then _merge_zip_hrr=2;
  else if  a &  b then _merge_zip_hrr=3;

  * keep only records in the zip-code level file;
  if a;
run;
proc freq data=zip_hrr_state;
table _merge_zip_hrr;
run;
* drop the few with no matching HRR ID;
data zip_hrr_state;
  set zip_hrr_state;
  where _merge_zip_hrr=3;
  drop _merge_zip_hrr;
run;
* collapse to HRR-state-cell level;
proc sql;
	 create table max_hrr_state as
      select
	    2012 as year,
	    hrr_state,
        cell,
		/* 
		  use min function on the following group of vars
		  they are constant within "group by"
		  PROC SQL will not just return these variables
        */
		min(state_cd) as state,
		min(hrrname) as hrrname,
        min(age_cat) as age_cat,
        min(dual_cat) as dual_cat,
        min(mc_cat) as mc_cat,
        min(dis_cat) as dis_cat,
        min(ltss_cat) as ltss_cat,
        min(foster_cat) as foster_cat,
		
        sum(cell_n) as cell_n, 
        sum(d_cell_n) as d_cell_n, 
        sum(male) as male, 
        sum(died_n) as died_n,
		sum(mas_cash) as mas_cash, 
        sum(mas_mn) as mas_mn, 
        sum(mas_pov) as mas_pov, 
        sum(mas_oth) as mas_oth, 
        sum(mas_1115) as mas_1115,
		sum(boe_aged) as boe_aged, 
        sum(boe_disabled) as boe_disabled, 
        sum(boe_child) as boe_child, 
        sum(boe_adult) as boe_adult, 
		sum(boe_uchild) as boe_uchild, 
        sum(boe_uadult) as boe_uadult, 
        sum(boe_fchild) as boe_fchild,

		sum(elg_months) as elg_months,
        sum(mo_dual) as mo_dual, 
        sum(mo_mc) as mo_mc, 
        sum(mo_dsbl) as mo_dsbl, 
        sum(mo_ltss) as mo_ltss,

		sum(_0) as _0,
        sum(_1_5) as _1_5, 
        sum(_6_18) as _6_18,
        sum(_19_44) as _19_44, 
        sum(_45_64) as _45_64,
        sum(_65_84) as _65_84,
        sum(_85p) as _85p,

		sum(spending) as spending

		from zip_hrr_state
      group by hrr_state, cell;
quit;
* Attach AHRF data -- already transformed into HRR-state level statistics;
data max_hrr_state_v2;
  merge max_hrr_state       (in=a)
        ahrf_hrr.&ahrf_hrr. (in=b drop=state);
		                          * drop state from AHRF data -> there are some obs in AHRF not in MAX, use state from max data  *;
  by hrr_state;
  * create a flag for cases without matching AHRF data;
  if       a & ~b then _ahrf_hrr=1;
  else if ~a &  b then _ahrf_hrr=2;
  else if  a &  b then _ahrf_hrr=3;
run;
proc freq data=max_hrr_state_v2;
  table _ahrf_hrr/missing;
run;
proc sort data=max_hrr_state_v2;
  by _ahrf_hrr;
run;
proc print data=max_hrr_state_v2 noobs;
where _ahrf_hrr in (1,2);
var hrr_state cell cell_n _ahrf_hrr;
run;
* keep only records in MAX - drop obs only in ahrf - format hrr_state & recode ahrf merge flag to 1/0;
data max_hrr_state_v2;
  set max_hrr_state_v2;
  where _ahrf_hrr in (1,3);
  format hrr_state $ 6.;
  if _ahrf_hrr=3 then _ahrf_msg=0;
  else if _ahrf_hrr in (1,2) then _ahrf_msg=1;
run;
* Attach to spending stats;
proc sort data=max_hrr_state_v2;
by hrr_state cell;
run;
data max_hrr_state_v3;
  merge max_hrr_state_v2    (in=a)
        hrr_state_stats_v2  (in=b);
  by hrr_state cell;
  if       a & ~b then _merge=1;
  else if ~a &  b then _merge=2;
  else if  a &  b then _merge=3;
run;
proc freq data=max_hrr_state_v3;
  table _merge;
run;

* transformations, rename & order variables;
proc sql;
  create table max_hrr_state_v4 as
  select
  year,
  state,

  hrr_state,
  hrrname,

  cell,
  age_cat, 
  dual_cat,
  mc_cat,
  dis_cat, 
  ltss_cat, 
  foster_cat,

  mo_dual as dual_mon, 
  mo_mc as mc_mon,
  mo_dsbl as dis_mon, 
  mo_ltss as ltss_mon,
  elg_months as elg_mon,
  
  cell_n,
  d_cell_n,
  died_n,

  mas_cash as mas_cash_n,
  mas_cash_n/cell_n as mas_cash,
  mas_mn as mas_mn_n,
  mas_mn_n/cell_n as mas_mn,
  mas_pov as mas_pov_n,
  mas_pov_n/cell_n as mas_pov,
  mas_oth as mas_oth_n,
  mas_oth_n/cell_n as mas_oth, 
  mas_1115 as mas_1115_n,
  mas_1115_n/cell_n as mas_1115,
  boe_aged as boe_aged_n,
  boe_aged_n/cell_n as boe_aged,
  boe_disabled as boe_disabled_n,
  boe_disabled_n/cell_n as boe_disabled, 
  boe_child as boe_child_n,
  boe_child_n/cell_n as boe_child,
  boe_adult as boe_adult_n,
  boe_adult_n/cell_n as boe_adult,
  boe_uchild as boe_uchild_n,
  boe_uchild_n/cell_n as boe_uchild,
  boe_uadult as boe_uadult_n,
  boe_uadult_n/cell_n as boe_uadult,
  boe_fchild as boe_fchild_n,
  boe_fchild_n/cell_n as boe_fchild,

  male as male_n,
  male_n/cell_n as male,
  _0 as _0_n,
  _0_n/cell_n as _0,
  _1_5 as _1_5_n,
  _1_5_n/cell_n as _1_5,
  _6_18 as _6_18_n,
  _6_18_n/cell_n as _6_18,
  _19_44 as _19_44_n,
  _19_44_n/cell_n as _19_44,
  _45_64 as _45_64_n,
  _45_64_n/cell_n as _45_64,
  _65_84 as _65_84_n,
  _65_84_n/cell_n as _65_84,
  _85p as _85p_n,
  _85p_n/cell_n as _85p,

  _ahrf_msg,
  hos_hrr_state as hbeds_pop,
  md_hrr_state as md_pop,
  poverty_hrr_state as povrate,
  urate_hrr_state as urate,
  
  spending as mcd_spd,
  mcd_spd_bar as spd_avg,
  mcd_spd_se as spd_se,
  mcd_spd_TC_bar as spd_TC_avg,
  mcd_spd_TC_se as spd_TC_se,
  mcd_spd_p995 as spd_TC_max,

  /* max of TC or top coded value 99.5th percentile */
  mcd_spd_p10 as spd_p10,
  mcd_spd_p25 as spd_p25,
  mcd_spd_p50 as spd_p50,
  mcd_spd_p75 as spd_p75,
  mcd_spd_p90 as spd_p90,
  mcd_spd_p95 as spd_p95,
  mcd_spd_p99 as spd_p99,
  mcd_spd_max as spd_max

  from max_hrr_state_v3;
quit;

* LABELS & FORMATTING;
data max_hrr_state_v5;
  set max_hrr_state_v4;

  format age_cat age.
	     mc_cat mc.
	     dis_cat dis.
	     ltss_cat ltss.
	     dual_cat dual.
	     foster_cat foster.;

  label
  year       =	"Year"
  state      =	"State Abbreviation"

  hrr_state  =	"Unique HRR-State ID"
  hrrname    =  "HRR city & primary state"

  cell       =	"Unique cell ID"  
  age_cat    =	"Age Category"
  dual_cat   =	"Dual-Eligibility Category"
  mc_cat     =	"Managed Care Category"
  dis_cat	=	"Disability Category"
  ltss_cat	=	"LTSS Use Category"
  foster_cat	="	Foster Care Category"
  				
  dual_mon	=	"Number of Person Months of Dual Eligibility"
  mc_mon	=	"Number of Person Months of Managed Care Enrollment"
  dis_mon	=	"Number of Person Months of Disability"
  ltss_mon	=	"Number of Person Months of LTSS Use"
  elg_mon	=	"Number of Person Months of Eligibility"
  				
  cell_n	=	"Number of Beneficiaries"
  d_cell_n	=	"Number of Unique Statuses"
  died_n	=	"Number Dying in Year"
				
  mas_cash_n =	"MAS Cash Beneficiaries (N)"
  mas_cash	=	"MAS Cash Beneficiaries (%)"
  mas_mn_n	=	"MAS Medically Needy Beneficiaries (N)"
  mas_mn	=	"MAS Medically Needy Beneficiaries (%)"
  mas_pov_n	=	"MAS Poverty-Related Beneficiaries (N)"
  mas_pov	=	"MAS Poverty-Related Beneficiaries (%)"
  mas_oth_n	=	"MAS Other Beneficiaries (N)"
  mas_oth	=	"MAS Other Beneficiaries (%)"
  mas_1115_n  =	"MAS 1115 Expansion Beneficiaries (N)"
  mas_1115	=	"MAS 1115 Expansion Beneficiaries (%)"
  boe_aged_n =	"BOE Aged Beneficiaries (N)"
  boe_aged	=	"BOE Aged Beneficiaries (%)"
  boe_disabled_n ="BOE Disabled Beneficiaries (N)"
  boe_disabled	="BOE Disabled Beneficiaries (%)"
  boe_child_n	="BOE Child Beneficiaries (N)"
  boe_child	=	"BOE Child Beneficiaries (%)"
  boe_adult_n	="BOE Adult Beneficiaries (N)"
  boe_adult	=	"BOE Adult Beneficiaries (%)"
  boe_uchild_n ="BOE Child (Unemployed Adult) Beneficiaries (N)"
  boe_uchild =	"BOE Child (Unemployed Adult) Beneficiaries (%)"
  boe_uadult_n ="BOE Unemployed Adult Beneficiaries (N)"
  boe_uadult   ="BOE Unemployed Adult Beneficiaries (%)"
  boe_fchild_n ="BOE Foster Child Beneficiaries (N)"
  boe_fchild	="BOE Foster Child Beneficiaries (%)"
  
  male_n =  "Number of Male Beneficiaries (N)"
  male	=	"Number of Male Beneficiaries (%)"
  _0_n	=	"Number of Beneficiaries Age less than 1 year (N)"
  _0	=	"Number of Beneficiaries Age less than 1 year (%)"
  _1_5_n =	"Number of Beneficiaries Age 1 to 5 (N)"
  _1_5	=	"Number of Beneficiaries Age 1 to 5 (%)"
  _6_18_n=	"Number of Beneficiaries Age 6 to 18 (N)"
  _6_18=	"Number of Beneficiaries Age 6 to 18 (%)"
  _19_44_n ="Number of Beneficiaries Age 19 to 44 (N)"
  _19_44 =	"Number of Beneficiaries Age 19 to 44 (%)"
  _45_64_n= "Number of Beneficiaries Age 45 to 64 (N)"
  _45_64 =  "Number of Beneficiaries Age 45 to 64 (%)"
  _65_84_n= "Number of Beneficiaries Age 65 to 84 (N)"
  _65_84  = "Number of Beneficiaries Age 65 to 84 (%)"
  _85p_n  = "Number of Beneficiaries Age 85 and above (N)"
  _85p = 	"Number of Beneficiaries Age 85 and above (%)"
  
  _ahrf_msg = "Missing AHRF Data Flag"
  hbeds_pop	= "Number of hospital beds per 1k people, 2010	"
  md_pop	= "Number of physicians per 1k people, 2010	"
  urate	=	  "Unemployment rate, 2012	"
  povrate =	  "Rate of persons in poverty, 2012	"
  
  mcd_spd    =  "Total Annual Medicaid Spending"
  spd_avg	 =	"Mean Annual Spending per Enrollee"
  spd_se	 =	"Standard Error of Mean Annual Spending"
  spd_tc_avg =	"Mean Annual Spending per Enrollee (Top Coded)"
  spd_tc_se	 =	"Standard Error of Mean Annual Spending (Top Coded)"
  spd_tc_max =	"Maximum Annual Spending per Enrollee (Top Coded p99.5)"
  spd_p10	=	"10th Percentile of Annual Spending"
  spd_p25	=	"25th Percentile of Annual Spending"
  spd_p50	=	"50th Percentile of Annual Spending"
  spd_p75	=	"75th Percentile of Annual Spending"
  spd_p90	=	"90th Percentile of Annual Spending"
  spd_p95	=	"95th Percentile of Annual Spending"
  spd_p99	=	"99th Percentile of Annual Spending"
  spd_max	=	"Maximum Annual Spending per Enrollee"
  ;
run;

proc contents data=max_hrr_state_v5 order=varnum;
run;

* Output HRR-state cell level file;

* Version with all cells; 
data out.hrr_state_allcells_v&date.;
  set max_hrr_state_v5;
run;
* Stata version;
proc export data=max_hrr_state_v5
  outfile= "P:\MCD-SPVR\data\workspace\output\hrr_state_allcells_v&date..dta" replace;
run;

* Version with blocked out values for rows that represent less than 11 enrollees;
%let maxvars = dual_mon mc_mon dis_mon ltss_mon elg_mon cell_n d_cell_n died_n mas_: boe_: male: _0: _1_5: _6_18: _19_44: _45_64: _65_84: _85p: mcd_spd spd:;
data out.hrr_state_nosmallcells_v&date.;
  set max_hrr_state_v5;
  array max {*} &maxvars.;
  if cell_n<11 then do;
    do i=1 to dim(max);
      max(i)=.S;
    end;
  end;
  drop i;
run;
