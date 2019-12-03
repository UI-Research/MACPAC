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
	/*proc printto print="P:\MCD-SPVR\log\08_finalreports_&sysdate..lst"
	               log="P:\MCD-SPVR\log\08_finalreports_&sysdate..log" NEW;
	run;*/
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
%let msa = out.msa_2012_02nov2019;

%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.));
%let space_name = %sysfunc(date(),date9.);
%let year = 2012;
%let dum_tab = %str(ods excel options(sheet_interval="table"); ods exclude all; data _null_; file print; put _all_; run; ods select all;);
%let report_folder = P:\MCD-SPVR\reports;
* AHRF HRR-state level file -- check for updated file;
%let ahrf_hrr = ahrf_hrr_state_v09_25_2018;


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


ods excel file="&report_folder.\output_summarystats_&fname..xlsx";
ods excel options(sheet_name="msa" sheet_interval="none");
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
ods excel options(sheet_name="Spend Summary" sheet_interval="none");
proc sql;
	select substr(st_msa,1,2) as state, 
		sum(cell_n) as cell_n_tot format comma16., 
		sum(mcd_spd_tot) format dollar16. as mdcd_spd_tot, 
		sum(mcd_spd_tot)/sum(cell_n) format dollar16. as spd_per_n,
		sum(mcd_spd_tot_tc) format dollar16. as mdcd_spd_tot_tc, 
		sum(mcd_spd_tot_tc)/sum(cell_n) format dollar16. as spd_tc_per_n,
		count(distinct substr(st_msa,3)) as num_MSAs
	from &msa.
	group by calculated state;
quit;

&dum_tab.;
ods excel options(sheet_name="contents" sheet_interval="none");
proc contents data=&msa.;run;
ods excel close;
/*export a stata copy;
proc export data=out.msa_allcells_&date.
  outfile= "P:\MCD-SPVR\data\workspace\output\msa_allcells_&date..dta" replace;
run;
*/

proc sql;
	create table temp as
	select *
	from &msa.
	where mcd_spd_tot > 19712419;
quit;

proc freq data=temp; 
	tables cell_n*mcd_spd_tot/list missing;
run;

proc printto;run;
