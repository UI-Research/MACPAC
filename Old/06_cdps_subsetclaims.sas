/*******************************************************************************************************************/ 
/*	Purpose: Aggregate cleaned up MAX data from 01_studypop_analyticfile with CDPS data and AHRF and MSA data 
/*			on user-input geographic variable			
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)
/*	Notes: 
/*		1) Collapse macros for easier manipulation
/* 		2) This version includes only those claims associated with a diagnosis related to CDPS score
/*******************************************************************************************************************/ 
/*Options to change*/
%macro prod();
	options obs=MAX;
	/*Log*/
	proc printto print="P:\MCD-SPVR\log\06_cdps_subsetclaims_&sysdate..lst"
	               log="P:\MCD-SPVR\log\06_cdps_subsetclaims_&sysdate..log" NEW;
	run;
%mend;

%macro test();	
	options obs=MAX;
	/*Log*/
	proc printto;run;
%mend;

%prod();
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
/*proc sql;
	select count(*)
	from space.max_cdpsclaims;
quit;
proc sql;
	select count(*)
	from space.id_pop_25feb2019;
quit;

/****************/
/* Add CDPS data*/
/****************/
	*1 concatenate state cdps files (DATA) to get previously calculated cdps scores;
	%macro do_states();
		%do i=1 %to 56;
			%if &i. ne 3 and &i. ne 7 and &i. ne 14 and &i. ne 43 and &i. ne 52 %then /*FIPS codes 3,7,14, 42, and 52 do not exist*/
				%do;
					%let state = %sysfunc(fipstate(&i));
					%put &state.;
					proc sql;
						create table temp_&state. as
						select *
						from scores.cdps_asth_&state. a inner join space.max_cdpsclaims b
						on a.recipno = b.recipno;
					quit;
				%end;
		%end;
	%mend;
	%do_states();

	data space.subset_max_plus_cdps;
		set temp_:;
	run;

proc sql;
	*drop table space.subset_max_plus_cdps;

	create table space.subset_max_plus_cdps as 
	select *
	from space.id_pop_25feb2019 a right join space.cdps_allst b
	on a.recipno = b.recipno;
quit;

