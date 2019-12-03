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
libname library "P:\MCD-SPVR\data\workspace\"; * includes format file;
libname cpds_wt  "P:\MCD-SPVR\data\NO_PII\CDPS_WEIGHTS";
libname  scores  "P:\MCD-SPVR\data\workspace\CDPS_SCORES";
/*Options to change*/
%include "P:\MCD-SPVR\programs\02_Assemble_Data_Files\sandbox\ud_formats.sas";
/* Macro vars to change*/
%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.));
%let space_name = %sysfunc(date(),date9.);
%let new_data = out.dia2l_msa_2012; /*input data file from 01_studypop_analyticfile*/
/*%let hrr_data = out.hrr_2012_06feb2019; 
%let old_data = out.msa_nosmallcells_10_30_18;*/
%let dum_tab = %str(ods excel options(sheet_interval="table"); ods exclude all; data _null_; file print; put _all_; run; ods select all;);
%let report_folder = P:\MCD-SPVR\reports;

%macro elig_sum(in_age=);
	proc odstext;
	  p "New Age Breakdown for Age Cat &in_age." / style=[color=red font_weight=bold];
	run;
	proc sql;
		title New Age Breakdown for Age Cat &in_age.;
		select age_servicetype, count(age_servicetype) as cell_age_n, sum(_0_n) as sum_under_1, sum(_1_5_n) as sum_1_to_5, sum(_6_18_n) as sum_6_to_18,
				sum(_19_44_n) as sum_19_to_44, sum(_45_64_n) as sum_45_64, sum(_65_84_n) as sum_65_to_84, sum(_85p) as sum_85plus,
				sum(boe_child_n) as sum_boe_child, sum(boe_fchild_n) as sum_boe_fchild, sum(boe_uchild_n) as sum_boe_uchild, 
				sum(boe_adult_n) as sum_boe_adult, sum(boe_uadult_n) as sum_boe_uadult,
				sum(boe_aged_n) as sum_boe_aged, sum(boe_disabled_n) as sum_boe_dis,
				sum(d_servicetype_n) as sum_d_cell, sum(died_n) as sum_died, 
				sum(mc_mon) as sum_mc_mon,sum(ltss_mon) as sum_ltss_mon, sum(dis_mon) as sum_dis_mon, sum(dual_mon) as sum_dual_mon, sum(elg_mon) as sum_elg_mon, 
				sum(mas_1115_n) as sum_mas_1115, sum(mas_cash_n) as sum_mas_cash, sum(mas_mn_n) as sum_mas_mn, sum(mas_oth_n) as sum_mas_oth, sum(mas_pov_n) as sum_mas_pov,
				sum(mcd_spd_tot) as sum_mcd_spd_tot, 
				sum(male_n) as sum_male, sum(beds) as sum_beds, sum(md) as sum_md
		from &new_data.
		where age_cat=&in_age.
		group by age_servicetype;
	quit;
	proc odstext;
	  p "Old Age Breakdown for Age Cat &in_age." / style=[color=red font_weight=bold];
	run;
	proc sql;
		title Old Age Breakdown for Age Cat &in_age.;
		select cell_age, count(cell_age) as cell_age_n, sum(_0_n) as sum_under_1, sum(_1_5_n) as sum_1_to_5, sum(_6_18_n) as sum_6_to_18,
				sum(_19_44_n) as sum_19_to_44, sum(_45_64_n) as sum_45_64, sum(_65_84_n) as sum_65_to_84, sum(_85p) as sum_85plus,
				sum(boe_child_n) as sum_boe_child, sum(boe_fchild_n) as sum_boe_fchild, sum(boe_uchild_n) as sum_boe_uchild, 
				sum(boe_adult_n) as sum_boe_adult, sum(boe_uadult_n) as sum_boe_uadult,
				sum(boe_aged_n) as sum_boe_aged, sum(boe_disabled_n) as sum_boe_dis,
				sum(d_cell_n) as sum_d_cell, sum(died_n) as sum_died, sum(dis_mon) as sum_dis_mon, sum(dual_mon) as sum_dual_mon, sum(elg_mon) as sum_elg_mon,
				sum(ltss_mon) as sum_ltss_mon, sum(male_n) as sum_male,
				sum(mas_1115_n) as sum_mas_1115, sum(mas_cash_n) as sum_mas_cash, sum(mas_mn_n) as sum_mas_mn, sum(mas_oth_n) as sum_mas_oth, sum(mas_pov_n) as sum_mas_pov,
				sum(mc_mon) as sum_mc_mon, sum(mcd_spd_tot) as sum_mcd_spd_tot, 
				sum(beds) as sum_beds, sum(md) as sum_md
		from &old_data.
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
	proc odstext;
	  p "Old Data: Categorical Variables for Age Cat &in_age." / style=[color=red font_weight=bold];
	run;
	proc freq data=&old_data.;
		title "Old Data: Categorical Variables for Age Cat &in_age.";
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
		select age_servicetype, count(age_servicetype) as cell_age_n, mean(&rate_var.) as mean_&rate_var., median(&rate_var.) as median_&rate_var., min(&rate_var.) as min_&rate_var.,
			max(&rate_var.) as max_&rate_var.
		from &new_data.
		where age_cat =  &in_age.
		group by age_servicetype;
	quit;
	proc odstext;
	  p "Old Data: Rate Variables for Age Cat &in_age." / style=[color=red font_weight=bold];
	run;
	proc sql;
		title "Old Data: Rate Variables for Age Cat &in_age.";
		select cell_age, count(cell_age) as cell_age_n, mean(&rate_var.) as mean_&rate_var., median(&rate_var.) as median_&rate_var., min(&rate_var.) as min_&rate_var.,
			max(&rate_var.) as max_&rate_var.
		from &old_data.
		where age_cat = &in_age.
		group by cell_age;
	quit;
%mend;
/*
ods excel file="&report_folder.\output_comparison_&fname..xlsx";
ods excel options(sheet_name="proc compare" sheet_interval="none");
proc compare base=&new_data. compare=&old_data.;
   title 'Comparing Two Data Sets: Basic Compare';
run;
title;

&dum_tab.;
ods excel options(sheet_name="total counts" sheet_interval="none");
	proc odstext;
	  p "New Data" / style=[color=red font_weight=bold];
	run;
	title New Data;
proc sql;	
	select age_cat, count(age_servicetype) as cell_age_N, sum(cell_n) as total_N
	from &new_data.
	group by age_cat;
quit;
	proc odstext;
	  p "Old Data" / style=[color=red font_weight=bold];
	run;
proc sql;
	title Old Data;
	select age_cat, count(cell_age) as cell_age_N, sum(cell_n) as total_N
	from &old_data.
	group by age_cat;
quit;

&dum_tab.;
ods excel options(sheet_name="child" sheet_interval="none");
%elig_sum(in_age=1);

&dum_tab.;
ods excel options(sheet_name="adult" sheet_interval="none");
%elig_sum(in_age=2);

&dum_tab.;
ods excel options(sheet_name="senior" sheet_interval="none");
%elig_sum(in_age=3);

&dum_tab.;


ods excel options(sheet_name="child categorical" sheet_interval="none");
%category_sum(in_age=1);

&dum_tab.;
ods excel options(sheet_name="adult categorical" sheet_interval="none");
%category_sum(in_age=1);

&dum_tab.;
ods excel options(sheet_name="senior categorical" sheet_interval="none");
%category_sum(in_age=3);


&dum_tab.;
ods excel options(sheet_name="povrate" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=povrate);
%numeric_sum(in_age=2,rate_var=povrate);
%numeric_sum(in_age=3,rate_var=povrate);

&dum_tab.;
ods excel options(sheet_name="urate" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=urate);
%numeric_sum(in_age=2,rate_var=urate);
%numeric_sum(in_age=3,rate_var=urate);

&dum_tab.;
ods excel options(sheet_name="spd_TC_max" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_TC_max);
%numeric_sum(in_age=2,rate_var=spd_TC_max);
%numeric_sum(in_age=3,rate_var=spd_TC_max);


&dum_tab.;
ods excel options(sheet_name="spd_avg" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_avg);
%numeric_sum(in_age=2,rate_var=spd_avg);
%numeric_sum(in_age=3,rate_var=spd_avg);

&dum_tab.;
ods excel options(sheet_name="spd_avg_TC" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_avg_TC);
%numeric_sum(in_age=2,rate_var=spd_avg_TC);
%numeric_sum(in_age=3,rate_var=spd_avg_TC);

&dum_tab.;
ods excel options(sheet_name="spd_max" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_max);
%numeric_sum(in_age=2,rate_var=spd_max);
%numeric_sum(in_age=3,rate_var=spd_max);

&dum_tab.;
ods excel options(sheet_name="spd_p10" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_p10);
%numeric_sum(in_age=2,rate_var=spd_p10);
%numeric_sum(in_age=3,rate_var=spd_p10);

&dum_tab.;
ods excel options(sheet_name="spd_p25" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_p25);
%numeric_sum(in_age=2,rate_var=spd_p25);
%numeric_sum(in_age=3,rate_var=spd_p25);

&dum_tab.;
ods excel options(sheet_name="spd_p50" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_p50);
%numeric_sum(in_age=2,rate_var=spd_p50);
%numeric_sum(in_age=3,rate_var=spd_p50);

&dum_tab.;
ods excel options(sheet_name="spd_p75" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_p75);
%numeric_sum(in_age=2,rate_var=spd_p75);
%numeric_sum(in_age=3,rate_var=spd_p75);

&dum_tab.;
ods excel options(sheet_name="spd_p90" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_p90);
%numeric_sum(in_age=2,rate_var=spd_p90);
%numeric_sum(in_age=3,rate_var=spd_p90);

&dum_tab.;
ods excel options(sheet_name="spd_p95" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_p95);
%numeric_sum(in_age=2,rate_var=spd_p95);
%numeric_sum(in_age=3,rate_var=spd_p95);

&dum_tab.;
ods excel options(sheet_name="spd_p99" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_p99);
%numeric_sum(in_age=2,rate_var=spd_p99);
%numeric_sum(in_age=3,rate_var=spd_p99);

&dum_tab.;
ods excel options(sheet_name="spd_se" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_se);
%numeric_sum(in_age=2,rate_var=spd_se);
%numeric_sum(in_age=3,rate_var=spd_se);

&dum_tab.;
ods excel options(sheet_name="spd_se_TC" sheet_interval="none");
%numeric_sum(in_age=1,rate_var=spd_se_TC);
%numeric_sum(in_age=2,rate_var=spd_se_TC);
%numeric_sum(in_age=3,rate_var=spd_se_TC);
ods excel close;
*/
