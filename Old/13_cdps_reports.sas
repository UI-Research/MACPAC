/*******************************************************************************************************************/ 
/*	Purpose: Evaluate PS file data quality for CDPS chronic conditions flag subpopulations			
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak
/*	Notes: 
/*		1) Collapse macros for easier manipulation
/*******************************************************************************************************************/ 
/*Options to change*/
%macro prod();
	options obs=MAX;
	/*Log*/
	proc printto print="P:\MCD-SPVR\log\05_cdps_summaries_&sysdate..lst"
	               log="P:\MCD-SPVR\log\05_cdps_summaries_&sysdate..log" NEW;
	run;
%mend;

%macro test();	
	options obs=5000000;
	/*Log*/
	proc printto;run;
%mend;

/*%prod();*/
%test();

/*Libraries*/
libname data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname data_ot    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS\OT";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;
libname cpds_wt "P:\MCD-SPVR\data\NO_PII\CDPS_WEIGHTS";
libname scores  "P:\MCD-SPVR\data\workspace\CDPS_SCORES";
libname ahrf_hrr "\\sas1_alt\MCD-SPVR\data\NO_PII\HRR\workspace";

%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.));
%let space_name = %sysfunc(date(),date9.);
%let indata = space.max2012_cdps_subset; /*from 05_*/
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

/* cdps flags of interest
dia2l
CAREL /*hypertension*
CANL /*colon, prostate, cervical cancer*
PRGCMP /*completed pregnancy*
PSYL /*depression, anxiety, phobia*
/*PULL /*asthma, this needs more work*/

/*Vars of interest
Inpatient stays (TOT_IP_STAY_CNT)
Inpatient spending (medicaid) (FFS_PYMT_AMT_01)
Physician and clinic (combined) claims (FFS_CLM_CNT_08+FFS_CLM_CNT_12)phys_clin_claims
Physician and clinic (combined) spending (FFS_PYMT_AMT_08+FFS_PYMT_AMT_12)phys_clin_spending
Prescription drug claims(FFS_CLM_CNT_16)
Prescription drug spending (FFS_PYMT_AMT_16)
Outpatient hospital (MSIS_TOS = 7)
Nursing facility (ot_MSIS_TOS = 11)
*/

/*break up the data in age categories*/
proc sql;
	create table child as
	select *
	from &indata.
	where age_cat = 1;

	create table adult as
	select *
	from &indata.
	where age_cat = 2;

	create table senior as
	select *
	from &indata.
	where age_cat = 3;
quit;

/*macro to create top coded spending variables for each CDPS flag*/
%macro create_tcspending_vars(agecat_dat, cdpsflag, outdata);
	proc univariate data=&agecat_dat. noprint;
		where &cdpsflag. = 1;
		class age_servicetype;
		var FFS_PYMT_AMT_01 phys_clin_spending FFS_PYMT_AMT_16;
		output out=spend_cap
		pctlpts = 99.5
		pctlpre=ffspymt_01_p physclin_p ffspymt_16_p;
	run;

	/*put stats together*/
	proc sql;
	  create table &outdata. AS
		select 
			a.*,
			b.ffspymt_01_p99_5,
			case 
				when a.FFS_PYMT_AMT_01> b.ffspymt_01_p99_5 then ffspymt_01_p99_5
				else FFS_PYMT_AMT_01
			end as ffspymt_01_TC,
			case 
				when a.phys_clin_spending> b.physclin_p99_5 then physclin_p99_5
				else phys_clin_spending
			end as physclin_spd_TC,
			case 
				when a.FFS_PYMT_AMT_16> b.ffspymt_16_p99_5 then ffspymt_16_p99_5
				else FFS_PYMT_AMT_16
			end as ffspymt_16_TC
		from &agecat_dat. a left join spend_cap b  
		on a.age_servicetype=b.age_servicetype
		where &cdpsflag. = 1;
	  quit;
%mend;

/*all data*
%create_tcspending_vars(agecat_dat=max_lt_ot_cdps, cdpsflag = dia2l, outdata=space.all_dia2l);
%create_tcspending_vars(agecat_dat=max_lt_ot_cdps, cdpsflag = CAREL, outdata=space.all_carel);
%create_tcspending_vars(agecat_dat=max_lt_ot_cdps, cdpsflag = CANL, outdata=space.all_canl);
%create_tcspending_vars(agecat_dat=max_lt_ot_cdps, cdpsflag = PRGCMP, outdata=space.all_prgcmp);
%create_tcspending_vars(agecat_dat=max_lt_ot_cdps, cdpsflag = PSYL, outdata=space.all_psyl);
/*%create_tcspending_vars(agecat_dat=all, cdpsflag = PULL, outdata=space.all_asthma);*/

/*top coding for child data*/
%create_tcspending_vars(agecat_dat=child, cdpsflag = dia2l, outdata=child_dia2l);
%create_tcspending_vars(agecat_dat=child, cdpsflag = CAREL, outdata=child_carel);
%create_tcspending_vars(agecat_dat=child, cdpsflag = CANL, outdata=child_canl);
%create_tcspending_vars(agecat_dat=child, cdpsflag = PRGCMP, outdata=child_prgcmp);
%create_tcspending_vars(agecat_dat=child, cdpsflag = PSYL, outdata=child_psyl);
/*%create_tcspending_vars(agecat_dat=child, cdpsflag = PULL, outdata=child_asthma);*/

/*top coding for adult data*/
%create_tcspending_vars(agecat_dat=adult, cdpsflag = dia2l, outdata=adult_dia2l);
%create_tcspending_vars(agecat_dat=adult, cdpsflag = CAREL, outdata=adult_carel);
%create_tcspending_vars(agecat_dat=adult, cdpsflag = CANL, outdata=adult_canl);
%create_tcspending_vars(agecat_dat=adult, cdpsflag = PRGCMP, outdata=adult_prgcmp);
%create_tcspending_vars(agecat_dat=adult, cdpsflag = PSYL, outdata=adult_psyl);
/*%create_tcspending_vars(agecat_dat=adult, cdpsflag = PULL, outdata=adult_asthma);*/

/*top coding for senior data*/
%create_tcspending_vars(agecat_dat=senior, cdpsflag = dia2l, outdata=senior_dia2l);
%create_tcspending_vars(agecat_dat=senior, cdpsflag = CAREL, outdata=senior_carel);
%create_tcspending_vars(agecat_dat=senior, cdpsflag = CANL, outdata=senior_canl);
%create_tcspending_vars(agecat_dat=senior, cdpsflag = PRGCMP, outdata=senior_prgcmp);
%create_tcspending_vars(agecat_dat=senior, cdpsflag = PSYL, outdata=senior_psyl);
/*%create_tcspending_vars(agecat_dat=senior, cdpsflag = PULL, outdata=senior_asthma);*/

/*macro to create summary tables for each variable*/
%macro var_summary(cdpsflag, agecat_dat, variable, label);
	proc odstext;
	  p propcase("&label.") / style=[color=black font_weight=bold];
	run;

	proc sql;
		select age_servicetype, 
			sum(&variable) label="Total &label.",
			mean(&variable) label="Mean &label. per beneficiary",
			median(&variable) label="Median &label. per beneficiary",
			min(&variable) label="Minimum &label. per beneficiary",
			max(&variable) label="Maximum &label. per beneficiary",
			std(&variable) label="Std dev of &label. per beneficiary",
			nmiss(&variable) label="Missing &label."
		from &agecat_dat.
		where &cdpsflag. = 1
		group by age_servicetype
		order by age_servicetype;
	quit;
%mend;

%macro cdpsflag_wkbook(cdpsflag, cdpsflag_label, workbook_name);
ods excel file="&report_folder.\&workbook_name._&space_name..xlsx";
ods excel options(sheet_name="child" sheet_interval="none");
	proc odstext;
	  p "&cdpsflag_label: Age_Servicetype Category Definitions with Total N's in Each Category" / style=[color=black font_weight=bold];
	run;
	proc sql;
		select cell_type1, cell_type2, cell_type3, cell_type4, age_servicetype, 
			sum(cell_n) as cell_n,
			mean(CDPS_SCORE) label="Mean CDPS Score"
		from child_&cdpsflag.
		where &cdpsflag. = 1
		group by cell_type1, cell_type2, cell_type3, cell_type4, age_servicetype;
	quit;
	
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=child_&cdpsflag., variable=TOT_IP_STAY_CNT, label=inpatient stays);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=child_&cdpsflag., variable=FFS_PYMT_AMT_01, label=inpatient spending);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=child_&cdpsflag., variable=ffspymt_01_TC, label=inpatient spending top coded);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=child_&cdpsflag., variable=phys_clin_claims, label=physician and clinic claims);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=child_&cdpsflag., variable=phys_clin_spending, label=physician and clinic spending);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=child_&cdpsflag., variable=physclin_spd_TC, label=physician and clinic spending top coded);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=child_&cdpsflag., variable=FFS_CLM_CNT_16, label=prescription drug claims);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=child_&cdpsflag., variable=FFS_PYMT_AMT_16, label=prescription drug spending);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=child_&cdpsflag., variable=ffspymt_16_TC, label=prescription drug spending top coded);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=child_&cdpsflag., variable=lt_MSIS_TOS, label=outpatient hospital);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=child_&cdpsflag., variable=ot_MSIS_TOS, label=nursing facility);


/*Add dummy table*/
&dum_tab.;
ods excel options(sheet_name="adult" sheet_interval="none");

	proc odstext;
	  p "&cdpsflag_label: Age_Servicetype Category Definitions with Total N's in Each Category" / style=[color=black font_weight=bold];
	run;
	proc sql;
		select cell_type1, cell_type2, cell_type3, cell_type4, age_servicetype, 
			sum(cell_n) as cell_n,
			mean(CDPS_SCORE) label="Mean CDPS Score"
		from adult_&cdpsflag.
		where &cdpsflag. = 1 
		group by cell_type1, cell_type2, cell_type3, cell_type4, age_servicetype;
	run;
	
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=adult_&cdpsflag., variable=TOT_IP_STAY_CNT, label=inpatient stays);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=adult_&cdpsflag., variable=FFS_PYMT_AMT_01, label=inpatient spending);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=adult_&cdpsflag., variable=ffspymt_01_TC, label=inpatient spending top coded);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=adult_&cdpsflag., variable=phys_clin_claims, label=physician and clinic claims);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=adult_&cdpsflag., variable=phys_clin_spending, label=physician and clinic spending);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=adult_&cdpsflag., variable=physclin_spd_TC, label=physician and clinic spending top coded);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=adult_&cdpsflag., variable=FFS_CLM_CNT_16, label=prescription drug claims);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=adult_&cdpsflag., variable=FFS_PYMT_AMT_16, label=prescription drug spending);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=adult_&cdpsflag., variable=ffspymt_16_TC, label=prescription drug spending top coded);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=adult_&cdpsflag., variable=lt_MSIS_TOS, label=outpatient hospital);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=adult_&cdpsflag., variable=ot_MSIS_TOS, label=nursing facility);


/*Add dummy table*/
&dum_tab.;
ods excel options(sheet_name="senior" sheet_interval="none");

	proc odstext;
	  p "&cdpsflag_label: Age_Servicetype Category Definitions with Total N's in Each Category" / style=[color=black font_weight=bold];
	run;
	proc sql;
		select cell_type1, cell_type2, cell_type3, cell_type4, age_servicetype, 
			sum(cell_n) as cell_n,
			mean(CDPS_SCORE) label="Mean CDPS Score"
		from senior_&cdpsflag.
		where &cdpsflag. = 1
		group by cell_type1, cell_type2, cell_type3, cell_type4, age_servicetype;
	run;
	
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=senior_&cdpsflag., variable=TOT_IP_STAY_CNT, label=inpatient stays);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=senior_&cdpsflag., variable=FFS_PYMT_AMT_01, label=inpatient spending);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=senior_&cdpsflag., variable=ffspymt_01_TC, label=inpatient spending top coded);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=senior_&cdpsflag., variable=phys_clin_claims, label=physician and clinic claims);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=senior_&cdpsflag., variable=phys_clin_spending, label=physician and clinic spending);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=senior_&cdpsflag., variable=physclin_spd_TC, label=physician and clinic spending top coded);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=senior_&cdpsflag., variable=FFS_CLM_CNT_16, label=prescription drug claims);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=senior_&cdpsflag., variable=FFS_PYMT_AMT_16, label=prescription drug spending);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=senior_&cdpsflag., variable=ffspymt_16_TC, label=prescription drug spending top coded);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=senior_&cdpsflag., variable=lt_MSIS_TOS, label=outpatient hospital);
	%var_summary(cdpsflag=&cdpsflag., agecat_dat=senior_&cdpsflag., variable=ot_MSIS_TOS, label=nursing facility);
ods excel close;
%mend;


%cdpsflag_wkbook(cdpsflag=dia2l, cdpsflag_label=Uncomplicated Diabetes,workbook_name=dia2l)
%cdpsflag_wkbook(cdpsflag=CAREL, cdpsflag_label=Hypertension,workbook_name=carel)
%cdpsflag_wkbook(cdpsflag=CANL, cdpsflag_label=Colon Prostate and Cervical Cancers,workbook_name=canl)
%cdpsflag_wkbook(cdpsflag=PRGCMP, cdpsflag_label=Completed Pregnancy,workbook_name=prgcmp)
%cdpsflag_wkbook(cdpsflag=PSYL, cdpsflag_label=Depression Anxiety and Phobia,workbook_name=psyl)
/*%cdpsflag_wkbook(cdpsflag=PULL, cdpsflag_label="Uncomplicated Diabetes") needs work*/
