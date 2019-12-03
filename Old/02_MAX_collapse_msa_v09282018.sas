/* 
   Program: Collapse person-level data

   Author: Abby Norling-Ruggles, modified by Kyle Caswell , modified by Tim Waidmann

   Last updated: 09/28/2018

*/

* Date for version control;
%let date=09_28_18;
options obs=MAX;
* log;
PROC PRINTTO PRINT="P:\MCD-SPVR\log\02_max_collapse_cty_msa_&sysdate..lst"
               LOG="P:\MCD-SPVR\log\02_max_collapse_cty_msa_&sysdate..log" NEW;
/*
PROC PRINTTO PRINT="D:\temp\03_max_collapse_cty_msa_&sysdate..lst"
               LOG="D:\temp\03_max_collapse_cty_msa_&sysdate..log" NEW;
*/
RUN;

libname  data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname  space   "P:\MCD-SPVR\data\workspace";
libname  area    "P:\MCD-SPVR\data\NO_PII";
libname  out     "P:\MCD-SPVR\data\workspace\output" COMPRESS=YES;
*libname out "D:\temp";
libname  library "P:\MCD-SPVR\data\workspace\output";
*libname temp "D:\temp";

* Collapse to cell-MSA level;
  *join MSA indicator;

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
	run;

data person_2012;
set space.categories_full_2012;

if missing(age_0) then age_0=0; 
if missing(age_1_5) then age_1_5=0;
if missing(age_6_18) then age_6_18=0;
if missing(age_19_44) then age_19_44=0;
if missing(age_45_64) then age_45_64=0;
if missing(age_65_84) then age_65_84=0; 
if missing(age_85p) then age_85p=0; 
if missing(boe_aged) then boe_aged=0; 
if missing(boe_disabled) then boe_disabled=0; 
if missing(boe_child) then boe_child=0; 
if missing(boe_adult) then boe_adult=0; 
if missing(boe_uchild) then boe_uchild=0; 
if missing(boe_uadult) then boe_uadult=0; 
if missing(boe_fchild) then boe_fchild=0; 
if missing(mas_cash) then mas_cash=0; 
if missing(mas_mn) then mas_mn=0; 
if missing(mas_pov) then mas_pov=0; 
if missing(mas_oth) then mas_oth=0; 
if missing(mas_1115) then mas_1115=0; 

if age_0 + age_1_5 + age_6_18 + age_19_44 + age_45_64 + age_65_84 + age_85p ~=1 then delete;
if boe_aged + boe_disabled + boe_child + boe_adult + boe_uchild + boe_uadult+boe_fchild ~=1 then delete;
if mas_cash + mas_mn +mas_pov +mas_oth +mas_1115~=1 then delete;

if cell='1' then do; 		cell='01'; dual_cat=0; mc_cat=1; dis_cat=1; ltss_cat=1; foster_cat=0; end;
 else if cell='2' then do; 	cell='02'; dual_cat=0; mc_cat=1; dis_cat=1; ltss_cat=0; foster_cat=0; end;
 else if cell='3' then do; 	cell='03'; dual_cat=0; mc_cat=1; dis_cat=0; ltss_cat=1; foster_cat=0; end;
 else if cell='4' then do; 	cell='04'; dual_cat=0; mc_cat=1; dis_cat=0; ltss_cat=0; foster_cat=0; end;
 else if cell='5' then do; 	cell='05'; dual_cat=0; mc_cat=0; dis_cat=1; ltss_cat=1; foster_cat=0; end;
 else if cell='6' then do; 	cell='06'; dual_cat=0; mc_cat=0; dis_cat=1; ltss_cat=0; foster_cat=0; end;
 else if cell='7' then do; 	cell='07'; dual_cat=0; mc_cat=0; dis_cat=0; ltss_cat=1; foster_cat=0; end;
 else if cell='8' then do; 	cell='08'; dual_cat=0; mc_cat=0; dis_cat=0; ltss_cat=0; foster_cat=0; end;
 else if cell='9' then do; 	cell='09'; dual_cat=1; mc_cat=1; dis_cat=1; ltss_cat=1; foster_cat=0; end;
 else if cell='10' then do; 		   dual_cat=1; mc_cat=1; dis_cat=1; ltss_cat=0; foster_cat=0; end;
 else if cell='11' then do;		   dual_cat=1; mc_cat=1; dis_cat=0; ltss_cat=1; foster_cat=0; end;
 else if cell='12' then do;		   dual_cat=1; mc_cat=1; dis_cat=0; ltss_cat=0; foster_cat=0; end;
 else if cell='13' then do;		   dual_cat=1; mc_cat=0; dis_cat=1; ltss_cat=1; foster_cat=0; end;
 else if cell='14' then do;		   dual_cat=1; mc_cat=0; dis_cat=1; ltss_cat=0; foster_cat=0; end;
 else if cell='15' then do;		   dual_cat=1; mc_cat=0; dis_cat=0; ltss_cat=1; foster_cat=0; end;
 else if cell='16' then do;		   dual_cat=1; mc_cat=0; dis_cat=0; ltss_cat=0; foster_cat=0; end;
 else if cell='17' then do; 	age_cat=1; dual_cat=9; mc_cat=9; dis_cat=9; ltss_cat=9; foster_cat=1; end;
if not missing(cell) then do;
	if age_cat=1 then cell_age=catx('_','child',cell);
		else if age_cat=2 then cell_age=catx('_','adult',cell);
	   	else if age_cat=3 then cell_age=catx('_','senior',cell);
	end;

run;

proc sql ;
      create table max_msa_2012 as
      select  A.*, 
      B.st_msa,  
      B.cbsatitle
	FROM person_2012 A 
	LEFT JOIN ahrf_msa_xwalk B
	ON A.state_cd=B.state_cd AND A.EL_RSDNC_CNTY_CD_LTST=B.county_fips;
quit;
run;


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
		sum(age_85p)/sum(cell_n) as _85p, 
		sum(male*age_19_44) as _19_44m_n,
		sum(male*age_19_44)/sum(cell_n) as _19_44_m,
		sum((1-male)*age_19_44) as _19_44f_n,
		sum((1-male)*age_19_44)/sum(cell_n) as _19_44_f,
		sum(male*age_85p) as _85pm_n,
		sum(male*age_85p)/sum(cell_n) as _85p_m,
		sum((1-male)*age_85p) as _85pf_n,
		sum((1-male)*age_85p)/sum(cell_n) as _85p_f
		from max_msa_2012
		GROUP BY year, cell_age, state_cd, st_msa , cbsatitle, age_cat, cell, mc_cat, dis_cat, dual_cat, ltss_cat, foster_cat;
	quit;
	run;
	
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


data out.max_msafull_2012;
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
_19_44m_n ="Number of Male Beneficiaries Age 19 to 44 (N)"
_19_44_m ="Number of Male Beneficiaries Age 19 to 44 (%)"
_19_44f_n ="Number of Female Beneficiaries Age 19 to 44 (N)"
_19_44_f ="Number of Female Beneficiaries Age 19 to 44 (%)"
_85pm_n ="Number of Male Beneficiaries Age 85 and above (N)"
_85p_m ="Number of Male Beneficiaries Age 85 and above (%)"
_85pf_n ="Number of Female Beneficiaries Age 85 and above (N)"
_85p_f ="Number of Female Beneficiaries Age 85 and above (%)"
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
proc sort data=out.max_msafull_2012 out=out.max_msafull_2012;
by year state st_msa  cell_age;
run;

*drop observations missing category data;
data out.msa_allcells_&date. ;
	set out.max_msafull_2012;
	where ~missing(year) and ~missing(age_cat) and ~missing(cell_age) and ~missing(dual_cat) and ~missing(mc_cat) and ~missing(dis_cat) and ~missing(ltss_cat) and ~missing(st_msa);
run;

%let maxvars = dual_mon mc_mon dis_mon ltss_mon elg_mon cell_n d_cell_n died_n mas_: boe_: male: _0: _1_5: _6_18: _19_44: _45_64: _65_84: _85p: mcd_spd_tot spd:;

*mark too-small cells missing;
data out.msa_nosmallcells_&date. ;
	set out.msa_allcells_&date.;
  array max {*} &maxvars.;
  if cell_n<11 then do;
    do i=1 to dim(max);
      max(i)=.S;
    end;
  end;
  drop i;
run;

*export a stata copy;
proc export data=out.msa_allcells_&date.
  outfile= "P:\MCD-SPVR\data\workspace\output\msa_allcells_&date..dta" replace;
run;

proc export data=out.msa_nosmallcells_&date.
  outfile= "P:\MCD-SPVR\data\workspace\output\msa_nosmallcells_&date..dta" replace;
run;
