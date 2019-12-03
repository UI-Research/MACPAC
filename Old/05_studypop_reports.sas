/*******************************************************************************************************************/ 
/*	Purpose: Create summary table with sums and rates of select variables 
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)
/*	Notes: 
/*		1) Collapse macros for easier manipulation
/*		2) Dependent on MAX data formatting
/*******************************************************************************************************************/ 
libname data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;

* Date for version control;
%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.));
%let space_name = 10jan2019;
%let dum_tab = %str(ods excel options(sheet_interval="table"); ods exclude all; data _null_; file print; put _all_; run; ods select all;);
%let report_folder = P:\MCD-SPVR\reports;

/*latest_elg_code = coalescec(MAX_ELG_CD_MO_12, MAX_ELG_CD_MO_11, MAX_ELG_CD_MO_10, MAX_ELG_CD_MO_9, MAX_ELG_CD_MO_8, MAX_ELG_CD_MO_7, MAX_ELG_CD_MO_6, MAX_ELG_CD_MO_5, 
		MAX_ELG_CD_MO_4, MAX_ELG_CD_MO_3, MAX_ELG_CD_MO_2, MAX_ELG_CD_MO_1);

		latest_elg_code = EL_MAX_ELGBLTY_CD_LTST;*/
/*log*/
proc printto print="P:\MCD-SPVR\log\11_studypop_reports_&fname..lst"
               log="P:\MCD-SPVR\log\11_studypop_reports_&fname..log" new;
run;

/*print study "templates" aka proc contents*/
proc sql;
	create table pop_toprint like space.id_pop_&space_name.;
	*create table bene_id_toprint like space.finder_file_&space_name.;
quit;

ods excel file="&report_folder.\template_study_pop_&fname..xlsx";
ods excel options(sheet_name="study pop file");
	ods select Variables;
	proc contents data=space.id_pop_&space_name.;run;
ods excel options(sheet_name="finder file");
	ods select Variables;
	proc contents data=space.finder_file_&space_name.;run;
	ods select default;
ods excel close;

/**************************/
/*Create summary workbooks*/
/**************************/

%macro age_levels(indata=,indata_msg=,outdata=);
	proc sql;
		create table &outdata._child as
		select *
		from &indata.
		where age_cat=1;

		create table &outdata._child_msg as
		select *
		from &indata_msg.
		where age_cat=1;

		create table &outdata._adult as
		select *
		from &indata.
		where age_cat=2;

		create table &outdata._adult_msg as
		select *
		from &indata_msg.
		where age_cat=2;

		create table &outdata._senior as
		select *
		from &indata.
		where age_cat=3;

		create table &outdata._senior_msg as
		select *
		from &indata_msg.
		where age_cat=3;
	quit;
%mend;
/******************************************************************************/
%age_levels(indata=space.id_pop_&space_name.,indata_msg=space.id_pop_dropped_&space_name.,outdata=personlevel)

%macro get_sum_tables(indata,outdata); /*read in by print_summary_tables*/
	proc sql;
		create table &outdata. as
		select cell_type1, cell_type2, cell_type3, cell_type4, cell, age_cat, 
			sum(cell_n) as cell_n, 
	        sum(d_cell_n) as d_cell_n, 
			sum(pm_n) as pm_n,
	        sum(male) as male, 
	        sum(died_n) as died_n,
			sum(age_0) as _0, 
	        sum(age_1_5) as _1_5, 
	        sum(age_6_18) as _6_18, 
	        sum(age_19_44) as _19_44, 
	        sum(age_45_64) as _45_64, 
	        sum(age_65_84) as _65_84, 
	        sum(age_85p) as _85p,

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
			sum(EL_ELGBLTY_MO_CNT) as elg_months, 
	        sum(mo_dual) as mo_dual, 
	        sum(mo_mc) as mo_mc, 
	        sum(mo_dsbl) as mo_dsbl, 
	        sum(mo_ltss) as mo_ltss
			from &indata.
	      	group by cell_type1, cell_type2, cell_type3, cell_type4, cell, age_cat;
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
	options nolabel;
	proc sql;
		select *
		from &outdata.
		order by cell;
	quit;
	options label;
%mend;

%macro get_rate_tables(indata,outdata);/*read in by print_summary_tables*/
	proc sql;
		create table &outdata. as
		 select cell_type1, cell_type2, cell_type3, cell_type4, cell, age_cat, 
			sum(cell_n) as cell_n, 
	        sum(d_cell_n)/sum(cell_n) as d_cell_rt format=8.3,
			sum(pm_n)/sum(cell_n) as pm_rt format=8.3,
	        sum(male)/sum(cell_n) as male_rt format=8.3, 
	        sum(died_n)/sum(cell_n) as died_rt format=8.3, 
			sum(age_0)/sum(cell_n) as _0_rt format=8.3, 
	        sum(age_1_5)/sum(cell_n) as _1_5_rt format=8.3, 
	        sum(age_6_18)/sum(cell_n) as _6_18_rt format=8.3, 
	        sum(age_19_44)/sum(cell_n) as _19_44_rt format=8.3, 
	        sum(age_45_64)/sum(cell_n) as _45_64_rt format=8.3,  
	        sum(age_65_84)/sum(cell_n) as _65_84_rt format=8.3,  
	        sum(age_85p)/sum(cell_n) as _85p_rt format=8.3, 

			sum(mas_cash)/sum(cell_n) as mas_cash_rt format=8.3, 
	        sum(mas_mn)/sum(cell_n) as mas_mn_rt format=8.3, 
	        sum(mas_pov)/sum(cell_n) as mas_pov_rt format=8.3, 
	        sum(mas_oth)/sum(cell_n) as mas_oth_rt format=8.3,  
	        sum(mas_1115)/sum(cell_n) as mas_1115_rt format=8.3, 
			sum(boe_aged)/sum(cell_n) as boe_aged_rt format=8.3,  
	        sum(boe_disabled)/sum(cell_n) as boe_disabled_rt format=8.3, 
	        sum(boe_child)/sum(cell_n) as boe_child_rt format=8.3, 
	        sum(boe_adult)/sum(cell_n) as boe_adult_rt format=8.3,  
			sum(boe_uchild)/sum(cell_n) as boe_uchild_rt format=8.3, 
	        sum(boe_uadult)/sum(cell_n) as boe_uadult_rt format=8.3,  
	        sum(boe_fchild)/sum(cell_n) as boe_fchild_rt format=8.3,  

	        sum(TOT_MDCD_PYMT_AMT)/sum(cell_n) as spending_rt format=8.3, 
			sum(EL_ELGBLTY_MO_CNT)/sum(cell_n) as elg_months_rt format=8.3, 
	        sum(mo_dual)/sum(cell_n) as mo_dual_rt format=8.3, 
	        sum(mo_mc)/sum(cell_n) as mo_mc_rt format=8.3, 
	        sum(mo_dsbl)/sum(cell_n) as mo_dsbl_rt format=8.3, 
	        sum(mo_ltss)/sum(cell_n) as mo_ltss_rt format=8.3
			from &indata.
	      	group by cell_type1, cell_type2, cell_type3, cell_type4, cell, age_cat;
	data &outdata.;
		set &outdata.;
		label
			cell_type1="Medicaid Only, Dual, or Foster Care"
			cell_type2="MC, FFS, or Foster Care"
			cell_type3="Disability, No Disability, or Foster Care"
			cell_type4="LTSS, No LTSS, or Foster Care"
			cell="MAS/BOE/Foster Care Category"
			age_cat="Age Category"
			cell_n="Number of Beneficiaries"
			d_cell_rt="Rate of Unique Statuses"
			pm_rt="Rate of enrollment months during year"
			male_rt="Rate of Male Beneficiaries"
			died_rt="Rate Dying in Year"
			mas_cash_rt="Rate of MAS Cash Beneficiaries"
			mas_mn_rt="Rate of MAS Medically Needy Beneficiaries"
			mas_pov_rt="Rate of MAS Poverty-Related Beneficiaries"
			mas_oth_rt="Rate of MAS Other Beneficiaries"
			mas_1115_rt="Rate of MAS 1115 Exspansion Beneficiaries"
			boe_aged_rt="Rate of BOE Aged Beneficiaries"
			boe_disabled_rt="Rate of BOE Disabled Beneficiaries"
			boe_child_rt="Rate of BOE Child Beneficiaries"
			boe_adult_rt="Rate of BOE Adult Beneficiaries"
			boe_uchild_rt="Rate of BOE Child (Unemployed Adult) Beneficiaries"
			boe_uadult_rt="Rate of BOE Unemployed Adult Beneficiaries"
			boe_fchild_rt="Rate of BOE Foster Child Beneficiaries"
			spending_rt="Rate of Total Annual Spending across Beneficiaries"

			elg_months_rt="Rate of Person Months of Eligibility"
			_0_rt="Rate of Beneficiaries Age less than 1 year"
			_1_5_rt="Rate of Beneficiaries Age 1 to 5"
			_6_18_rt="Rate of Beneficiaries Age 6 to 18"
			_19_44_rt="Rate of Beneficiaries Age 19 to 44"
			_45_64_rt="Rate of Beneficiaries Age 45 to 64"
			_65_84_rt="Rate of Beneficiaries Age 65 to 84"
			_85p_rt="Rate of Beneficiaries Age 85 and above"
			mo_dual_rt="Rate of Person Months of Dual Eligibility"
			mo_mc_rt="Rate of Person Months of Managed Care Enrollment"
			mo_dsbl_rt="Rate of Person Months of Disability"
			mo_ltss_rt="Rate of Person Months of LTSS Use"
			mc_cat_rt="Rate of Managed Care Category"
			dis_cat_rt="Rate of Disability Category"
			dual_cat_rt="Rate of Dual-Eligibility Category"
			ltss_cat_rt="Rate of LTSS Use Category"
			foster_cat_rt="Rate of Foster Care Category"

			;
		run;
	options nolabel;
		proc sql;
			select *
			from &outdata.
			order by cell;
		quit;
		options label;
	quit;
%mend;



%macro print_summary_tables(indata=);
	ods excel file="&report_folder.\max stats_national_&fname..xlsx";
	ods excel options(sheet_name="child_raw" sheet_interval="none");
	%get_sum_tables(indata=&indata._child,outdata=child_sum);

	/*Add dummy table*/
	&dum_tab.;

	ods excel options(sheet_name="adult_raw" sheet_interval="none");
	%get_sum_tables(indata=&indata._adult,outdata=adult_sum);

	/*Add dummy table*/
	&dum_tab.;

	ods excel options(sheet_name="senior_raw" sheet_interval="none");
	%get_sum_tables(indata=&indata._senior,outdata=senior_sum);

	/*Add dummy table*/
	&dum_tab.;

	ods excel options(sheet_name="child_rates" sheet_interval="none");
	%get_rate_tables(indata=&indata._child,outdata=child_rate);

	/*Add dummy table*/
	&dum_tab.;

	ods excel options(sheet_name="adult_rates" sheet_interval="none");
	%get_rate_tables(indata=&indata._adult,outdata=adult_rate);

	/*Add dummy table*/
	&dum_tab.;

	ods excel options(sheet_name="senior_rates" sheet_interval="none");
	%get_rate_tables(indata=&indata._senior,outdata=senior_rate);

	/*Add dummy table*/
	&dum_tab.;

	ods excel options(sheet_name="dictionary" sheet_interval="none");
	proc sql;
		select name label="Variable", "" label="Related Variables", label label="Label", "" as denom label="Denominator", "" label="Value Label", "" label="Notes"
		from dictionary.columns
		where libname = "WORK" and memname = "CHILD_SUM"
		union
		select name label="Variable", "" label="Related Variables", label label="Label", case
					when name in ("d_cell_rt", "pm_rt","male_rt","died_rt","mcd_full_rt","mas_cash_rt","mas_mn_rt","mas_pov_rt",
									"mas_oth_rt","mas_1115_rt","boe_aged_rt","boe_disabled_rt","boe_child_rt","boe_adult_rt",
									"boe_uchild_rt","boe_uadult_rt","boe_fchild_rt","spending_rt","age_rt","_0_rt","_1_5_rt","_6_18_rt","_19_44_rt",
									"_45_64_rt","_65_84_rt","_85p_rt",) then "cell_n"
					when name in ("disabled_rt","cash_rt","chip_rt","ltss_rt") then "pm_n"
					else ""
				end as denom label = "Demoninator",
				"" label="Value Label", "" label="Notes"
		from dictionary.columns
		where libname = "WORK" and memname = "CHILD_RATE"
		order by denom, name;
	quit;

	/*Add dummy table*/
	&dum_tab.;

	ods excel options(sheet_name="total counts" sheet_interval="none");
	
	proc sql;
		create table counts as
		select count(*) as child_count format=comma12.,
			(select count(*) from &indata._child_msg) as child_msg_count format=comma12.,
			(select count(*) from &indata._adult) as adult_count format=comma12.,
			(select count(*) from &indata._adult_msg) as adult_msg_count format=comma12.,
			(select count(*) from &indata._senior) as senior_count format=comma12.,
			(select count(*) from &indata._senior_msg) as senior_msg_count format=comma12.
		from &indata._child;
		title Totals for Valid and Invalid Obs by Age and Total;
		select child_count, child_msg_count, 
			(1-((child_count-child_msg_count)/child_count))*100 as child_percent_msg, 
			adult_count, adult_msg_count, 
			(1-((adult_count-adult_msg_count)/adult_count))*100 as adult_percent_msg, 
			senior_count, senior_msg_count, 
			(1-((senior_count-senior_msg_count)/senior_count))*100 as senior_percent_msg, 
			sum(child_count, child_msg_count,adult_count,adult_msg_count,senior_count,senior_msg_count) as total format=comma12.
		from counts;
		title;
	quit;

	ods excel close;


	ods excel file="&report_folder.\max stats_national_msg_&fname..xlsx";
	ods excel options(sheet_name="child_raw" sheet_interval="none");
	%get_sum_tables(indata=&indata._child_msg,outdata=child_sum_msg);

	/*Add dummy table*/
	&dum_tab.;

	ods excel options(sheet_name="adult_raw" sheet_interval="none");
	%get_sum_tables(indata=&indata._adult_msg,outdata=adult_sum_msg);

	/*Add dummy table*/
	&dum_tab.;

	ods excel options(sheet_name="senior_raw" sheet_interval="none");
	%get_sum_tables(indata=&indata._senior_msg,outdata=senior_sum_msg);

	/*Add dummy table*/
	&dum_tab.;

	ods excel options(sheet_name="child_rates" sheet_interval="none");
	%get_rate_tables(indata=&indata._child_msg,outdata=child_rate_msg);

	/*Add dummy table*/
	&dum_tab.;

	ods excel options(sheet_name="adult_rates" sheet_interval="none");
	%get_rate_tables(indata=&indata._adult_msg,outdata=adult_rate_msg);

	/*Add dummy table*/
	&dum_tab.;

	ods excel options(sheet_name="senior_rates" sheet_interval="none");
	%get_rate_tables(indata=&indata._senior_msg,outdata=senior_rate_msg);

	/*Add dummy table*/
	&dum_tab.;

	ods excel options(sheet_name="dictionary" sheet_interval="none");
	proc sql;
		select name label="Variable", "" label="Related Variables", label label="Label", "" as denom label="Denominator", "" label="Value Label", "" label="Notes"
		from dictionary.columns
		where libname = "WORK" and memname = "CHILD_SUM_MSG"
		union
		select name label="Variable", "" label="Related Variables", label label="Label", case
					when name in ("d_cell_rt", "pm_rt","male_rt","died_rt","mcd_full_rt","mas_cash_rt","mas_mn_rt","mas_pov_rt",
									"mas_oth_rt","mas_1115_rt","boe_aged_rt","boe_disabled_rt","boe_child_rt","boe_adult_rt",
									"boe_uchild_rt","boe_uadult_rt","boe_fchild_rt","spending_rt","age_rt","_0_rt","_1_5_rt","_6_18_rt","_19_44_rt",
									"_45_64_rt","_65_84_rt","_85p_rt",) then "cell_n"
					when name in ("disabled_rt","cash_rt","chip_rt","ltss_rt") then "pm_n"
					else ""
				end as denom label = "Demoninator",
				"" label="Value Label", "" label="Notes"
		from dictionary.columns
		where libname = "WORK" and memname = "CHILD_RATE_MSG"
		order by denom, name;
	quit;

	ods excel close;
%mend;
/******************************************************************************/
%print_summary_tables(indata=personlevel);
