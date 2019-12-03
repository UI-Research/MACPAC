/*******************************************************************************************************************/ 
/*	Purpose: Using a yearly MAX input data set, create an analytic file with specified variables. 
/*				Fix invalid geographic information.
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)
/*	Notes: 
/*		Drop benes with partial Medicaid eligibility when cell type is determined (last observed month). Add a summary variable on the number of total partial benefit months in the cell.
/*		Include small cell size for internal purposes
/*		Include top-coded spending summary series (sum, mean, etc.)
/*		1) Collapse macros for easier manipulation
/*		2) Dependent on MAX data formatting
/*******************************************************************************************************************/ 
/*Options to change*/
%macro prod();
	options obs=MAX;
	/*Log*/
	proc printto print="P:\MCD-SPVR\log\03_studypop_finaltables_&sysdate..lst"
	               log="P:\MCD-SPVR\log\03_studypop_finaltables_&sysdate..log" NEW;
	run;
%mend;

%macro test();	
	options obs=10000;
	/*Log*/
	proc printto;run;
%mend;

%prod();
*%test();

* Macro variables for processing;
%let indata=space.temp_personlevel_geofixed;
%let personlevel=space.temp_personlevel;
%let outdata=space.id_pop;

%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.)); /*date+time for file names, to ensure files aren't overwritten*/
%let space_name = %sysfunc(date(),date9.); /*date for naming convention in space lib*/
%let dum_tab = %str(ods excel options(sheet_interval="table"); ods exclude all; data _null_; file print; put _all_; run; ods select all;); /*adds a dummy table to make a new worksheet ods excel*/
%let report_folder = P:\MCD-SPVR\reports; /*location of output reports*/
%let indata_year = 2012; /*claims year*/
%let age_date = 12,01,2012; /*date from which to calculate age. must be month,day,year*/

/*libraries*/
libname data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;

proc sql;
	drop table &outdata.,  &outdata._dropped ;
quit;

proc format library=library;
	value num_notzero
		. = 'Missing'
		0 = 'Zero'
		other = 'Not zero';
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
	value $elig_label
		  '00' = "NOT ELIGIBLE"
		  '11' = "AGED, CASH"
		  '12' = "BLIND/DISABLED, CASH"
		  '14' = "CHILD (NOT CHILD OF UNEMPLOYED ADULT, NOT FOSTER CARE CHILD)"
		  '15' = "ADULT (NOT BASED ON UNEMPLOYMENT STATUS)"
		  '16' = "CHILD OF UNEMPLOYED ADULT"
		  '17' = "UNEMPLOYED ADULT"
	      '21' = "AGED, MN" 
		  '22' = "BLIND/DISABLED, MN"
		  '24' = "CHILD, MN (FORMERLY AFDC CHILD, MN)"
		  '25' = "ADULT, MN (FORMERLY AFDC ADULT, MN)"
		  '31' = "AGED, POVERTY"
		  '32' = "BLIND/DISABLED, POVERTY"
		  '34' = "CHILD, POVERTY (INCLUDES MEDICAID EXPANSION SCHIP CHILDREN)"
		  '35' = "ADULT, POVERTY"
		  '3A' = "INDIVIDUAL COVERED UNDER THE BREAST AND CERVICAL CANCER PREVENTION ACT OF 2000, POVERTY"
		  '41' = "OTHER AGED"
		  '42' = "OTHER BLIND/DISABLED"
	 	  '44' = "OTHER CHILD"
		  '45' = "OTHER ADULT"
		  '48' = "FOSTER CARE CHILD"
		  '51' = "AGED, SECTION 1115 DEMONSTRATION EXPANSION"
		  '52' = "DISABLED, SECTION 1115 DEMONSTRATION EXPANSION"
		  '54' = "CHILD, SECTION 1115 DEMONSTRATION EXPANSION"
		  '55' = "ADULT, SECTION 1115 DEMONSTRATION EXPANSION"
		  '99' = "UNKNOWN ELIGIBILITY";
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
	value $ missfmt ' '="Missing"
			other="Not Missing";
	value nmissfmt . ="Missing"
			other="Not Missing";
quit;

/***********************/
/*Send data to perm lib*/
/***********************/
proc sql noprint;
	select name || "not in ('chip', 'nmcd', '')" into :valid_data separated by " or "
	from dictionary.columns
	where libname=upcase("space") and memname like upcase('%personlevel_geofixed%') and name like "servicetype_%";
quit;
proc sql;
	create table &outdata. as
	select *
	from &indata. (drop = county_miss cnty_match no_cnty_match no_zip_match valid_cnty valid_zip zip_code zip_match zip_miss max_st_cnty st_cnty rename=(cnty_fx = county zip_fx=zipcode))
	where not missing(age_cat) and (&valid_data.);
quit;

proc sql noprint;
	select name || "in ('chip', 'nmcd', '')" into :invalid_data separated by " and "
	from dictionary.columns
	where libname=upcase("space") and memname like upcase('%personlevel_geofixed%') and name like "servicetype_%";
quit;
proc sql;
	create table &outdata._dropped as
	select *
	from &indata. (drop = no_cnty_match no_zip_match valid_cnty valid_zip zip_code zip_match zip_miss rename = (cnty_fx = county zip_fx=zipcode))
	where missing(age_cat) or (&invalid_data.);
quit;

ods excel file="&report_folder.\dropped_summary_&fname..xlsx";
ods excel options(sheet_name="dropped summary" sheet_interval="none");

proc sql;
	select count(*) as num_bad_geo_dropped
	from &personlevel. a left join &indata. b
	on a.bene_id = b.bene_id
	where missing(b.bene_id);
quit;

proc sql noprint;
	select "age_cat*" ||name  into :ser_freq separated by " "
	from dictionary.columns
	where libname=upcase("space") and memname = upcase("id_pop_dropped") and name like "servicetype_%";
quit;

proc freq data=space.id_pop_dropped;
	title "Service type vs. age cat for dropped";
	tables &ser_freq. /list missing;
run;

ods excel close; 
/*create finder file
data space.finder_file;
	set space.id_pop (keep = bene_id cell_:); 
run;
*/
proc printto;run;
