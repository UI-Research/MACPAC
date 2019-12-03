/*******************************************************************************************************************/ 
/*	Purpose: Compare my data process results with previous data processing results			
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)
/*	Notes: 
/*		1) Collapse macros for easier manipulation
/*******************************************************************************************************************/ 
/*Libraries*/
libname data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;
libname cpds_wt  "P:\MCD-SPVR\data\NO_PII\CDPS_WEIGHTS";
libname  scores  "P:\MCD-SPVR\data\workspace\CDPS_SCORES";
/*Options to change*/
options obs=MAX;
options nolabel;   /* <== suppress labels for all variables */
/* Macro vars to change*/
%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.));
%let space_name = %sysfunc(date(),date9.);
%let new_data = space.id_pop_dropped_2jan2019; /*input data file from 01_studypop_analyticfile*/
%let dum_tab = %str(ods excel options(sheet_interval="table"); ods exclude all; data _null_; file print; put _all_; run; ods select all;);
%let report_folder = P:\MCD-SPVR\reports;

proc format;
	value missing_othernum
		. = 'Missing'
		0 = 'Missing'
		other = 'OK';
	value $missing_otherchar
		'' = 'Missing'
		' '  = 'Missing'
		other = 'OK';
	value $chip_status
		'chip' = 'chip'
		'nmcd'  = '-'
		'msg' = '-'
		other = 'OK';
quit;


proc freq data=&new_data. ;
	tables EL_MAX_ELGBLTY_CD_LTST*month_:/list missing;
run;

proc sql;
	create table stats as
	select EL_MAX_ELGBLTY_CD_LTST,age_cat,cell_age,dual_cat,mc_cat,dis_cat,ltss_cat,zipcode,county,
		month_1,month_2,month_3,month_4,month_5,month_6,month_7,month_8,month_9,month_10,month_11,month_12,
		sum(age_0,age_1_5 , age_6_18 , age_19_44 , age_45_64 , age_65_84 , age_85p) as sum_agegrp,
		sum(boe_aged , boe_disabled , boe_child , boe_adult , boe_uchild , boe_uadult,boe_fchild) as sum_boe,
		sum(mas_cash , mas_mn ,mas_pov ,mas_oth ,mas_1115) as sum_mas
		from &new_data.;
quit;

ods excel file="&report_folder.\output_comparison_&fname..xlsx";
ods excel options(sheet_name="total counts" sheet_interval="none");
proc freq data=stats;
	title "Obs Missing Key Variables for Age Cat 1";
	tables EL_MAX_ELGBLTY_CD_LTST*age_cat*cell_age*dual_cat*mc_cat*dis_cat*ltss_cat*zipcode*county*sum_agegrp*sum_boe*sum_mas/list missing;
	format age_cat dual_cat mc_cat dis_cat ltss_cat zipcode sum_agegrp sum_boe sum_mas missing_othernum. cell_age county $missing_otherchar.;
	where age_cat = 1;
run;

proc freq data=stats;
	title "Obs Missing Key Variables for Age Cat 2";
	tables age_cat*cell_age*dual_cat*mc_cat*dis_cat*ltss_cat*zipcode*county*sum_agegrp*sum_boe*sum_mas/list missing;
	format age_cat dual_cat mc_cat dis_cat ltss_cat zipcode sum_agegrp sum_boe sum_mas missing_othernum. cell_age county $missing_otherchar.;
	where age_cat = 2;
run;

proc freq data=stats;
	title "Obs Missing Key Variables for Age Cat 3";
	tables age_cat*cell_age*dual_cat*mc_cat*dis_cat*ltss_cat*zipcode*county*sum_agegrp*sum_boe*sum_mas/list missing;
	format age_cat dual_cat mc_cat dis_cat ltss_cat zipcode sum_agegrp sum_boe sum_mas missing_othernum. cell_age county $missing_otherchar.;
	where age_cat = 3;
run;

proc sql;
	select age_cat, count(*) as chip_valid_benes 
	from (
		select *
		from stats
		where (month_1 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_2 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_3 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_4 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_5 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_6 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_7 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_8 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_9 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_10 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_11 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_12 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17'))
		)
	where (month_1 = 'chip' or month_2= 'chip' or month_3= 'chip' or month_4= 'chip' or month_5= 'chip' or month_6= 'chip' or month_7= 'chip' or month_8= 'chip' or month_9= 'chip' or month_10= 'chip' or month_11= 'chip' or month_12 = 'chip')
	group by age_cat;
quit;

proc sql;
	select count(*) as chip_valid_benes 
	from (
		select *
		from stats
		where (month_1 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_2 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_3 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_4 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_5 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_6 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_7 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_8 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_9 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_10 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_11 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17') or month_12 in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17'))
		)
	where (month_1 = 'chip' or month_2= 'chip' or month_3= 'chip' or month_4= 'chip' or month_5= 'chip' or month_6= 'chip' or month_7= 'chip' or month_8= 'chip' or month_9= 'chip' or month_10= 'chip' or month_11= 'chip' or month_12 = 'chip')
	;
quit;

ods excel close;



%macro elig_sum(in_age=);
	proc odstext;
	  p "New Age Breakdown for Age Cat &in_age." / style=[color=red font_weight=bold];
	run;
	proc sql;
		title New Age Breakdown for Age Cat &in_age.;
		select cell_age, count(cell_age) as cell_age_n, sum(_0_n) as sum_under_1, sum(_1_5_n) as sum_1_to_5, sum(_6_18_n) as sum_6_to_18,
				sum(_19_44_n) as sum_19_to_44, sum(_45_64_n) as sum_45_64, sum(_65_84_n) as sum_65_to_84, sum(_85p) as sum_85plus,
				sum(boe_child_n) as sum_boe_child, sum(boe_fchild_n) as sum_boe_fchild, sum(boe_uchild_n) as sum_boe_uchild, 
				sum(boe_adult_n) as sum_boe_adult, sum(boe_uadult_n) as sum_boe_uadult,
				sum(boe_aged_n) as sum_boe_aged, sum(boe_disabled_n) as sum_boe_dis,
				sum(d_cell_n) as sum_d_cell, sum(died_n) as sum_died, 
				sum(mc_mon) as sum_mc_mon,sum(ltss_mon) as sum_ltss_mon, sum(dis_mon) as sum_dis_mon, sum(dual_mon) as sum_dual_mon, sum(elg_mon) as sum_elg_mon, 
				sum(mas_1115_n) as sum_mas_1115, sum(mas_cash_n) as sum_mas_cash, sum(mas_mn_n) as sum_mas_mn, sum(mas_oth_n) as sum_mas_oth, sum(mas_pov_n) as sum_mas_pov,
				sum(mcd_spd_tot) as sum_mcd_spd_tot, 
				sum(male_n) as sum_male, sum(beds) as sum_beds, sum(md) as sum_md
		from &new_data.
		where age_cat=&in_age.
		group by cell_age;
	quit;
%mend;

%macro category_sum(in_age=);
	proc odstext;
	  p "New Data: Categorical Variables for Age Cat &in_age." / style=[color=red font_weight=bold];
	run;
	proc freq data=&new_data.;
		title "New Data: Categorical Variables for Age Cat &in_age.";
		tables dis_cat dual_cat foster_cat ltss_cat mc_cat/list missing nocum;
		where age_cat = &in_age. ;
	run;
%mend;

%macro numeric_sum(in_age=,rate_var=);
	proc odstext;
	  p "New Data: Rate Variables for Age Cat &in_age." / style=[color=red font_weight=bold];
	run;
	proc sql;
		title "New Data: Rate Variables for Age Cat &in_age.";
		select cell_age, count(cell_age) as cell_age_n, mean(&rate_var.) as mean_&rate_var., median(&rate_var.) as median_&rate_var., min(&rate_var.) as min_&rate_var.,
			max(&rate_var.) as max_&rate_var.
		from &new_data.
		where age_cat =  &in_age.
		group by cell_age;
	quit;
%mend;

/*
ods excel file="&report_folder.\output_comparison_&fname..xlsx";
ods excel options(sheet_name="total counts" sheet_interval="none");
	proc odstext;
	  p "New Data" / style=[color=red font_weight=bold];
	run;
	title New Data;
proc sql;	
	select age_cat, count(cell_age) as cell_age_N, sum(cell_n) as total_N
	from &new_data.
	group by age_cat;
quit;
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="child" sheet_interval="none");
%elig_sum(in_age=1);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="adult" sheet_interval="none");
%elig_sum(in_age=2);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="senior" sheet_interval="none");
%elig_sum(in_age=3);
/*Add dummy table
&dum_tab.;


ods excel options(sheet_name="child categorical" sheet_interval="none");
%category_sum(in_age=1);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="adult categorical" sheet_interval="none");
%category_sum(in_age=1);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="senior categorical" sheet_interval="none");
%category_sum(in_age=3);

/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="povrate" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=povrate);
%numeric_sum(in_age=2,rate_var=povrate);
%numeric_sum(in_age=3,rate_var=povrate);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="urate" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=urate);
%numeric_sum(in_age=2,rate_var=urate);
%numeric_sum(in_age=3,rate_var=urate);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="spd_TC_max" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_TC_max);
%numeric_sum(in_age=2,rate_var=spd_TC_max);
%numeric_sum(in_age=3,rate_var=spd_TC_max);

/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="spd_avg" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_avg);
%numeric_sum(in_age=2,rate_var=spd_avg);
%numeric_sum(in_age=3,rate_var=spd_avg);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="spd_avg_TC" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_avg_TC);
%numeric_sum(in_age=2,rate_var=spd_avg_TC);
%numeric_sum(in_age=3,rate_var=spd_avg_TC);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="spd_max" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_max);
%numeric_sum(in_age=2,rate_var=spd_max);
%numeric_sum(in_age=3,rate_var=spd_max);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="spd_p10" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_p10);
%numeric_sum(in_age=2,rate_var=spd_p10);
%numeric_sum(in_age=3,rate_var=spd_p10);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="spd_p25" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_p25);
%numeric_sum(in_age=2,rate_var=spd_p25);
%numeric_sum(in_age=3,rate_var=spd_p25);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="spd_p50" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_p50);
%numeric_sum(in_age=2,rate_var=spd_p50);
%numeric_sum(in_age=3,rate_var=spd_p50);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="spd_p75" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_p75);
%numeric_sum(in_age=2,rate_var=spd_p75);
%numeric_sum(in_age=3,rate_var=spd_p75);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="spd_p90" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_p90);
%numeric_sum(in_age=2,rate_var=spd_p90);
%numeric_sum(in_age=3,rate_var=spd_p90);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="spd_p95" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_p95);
%numeric_sum(in_age=2,rate_var=spd_p95);
%numeric_sum(in_age=3,rate_var=spd_p95);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="spd_p99" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_p99);
%numeric_sum(in_age=2,rate_var=spd_p99);
%numeric_sum(in_age=3,rate_var=spd_p99);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="spd_se" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_se);
%numeric_sum(in_age=2,rate_var=spd_se);
%numeric_sum(in_age=3,rate_var=spd_se);
/*Add dummy table
&dum_tab.;
ods excel options(sheet_name="spd_se_TC" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_se_TC);
%numeric_sum(in_age=2,rate_var=spd_se_TC);
%numeric_sum(in_age=3,rate_var=spd_se_TC);
ods excel close;
