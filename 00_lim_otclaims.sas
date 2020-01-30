/*******************************************************************************************************************/ 
/*	Purpose: Limit OT claims to MAX TOS 51,52,53,54  	
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak
/*		1) Collapse macros for easier manipulation
/*	Notes: 
/*******************************************************************************************************************/ 
libname  data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname  space   "P:\MCD-SPVR\data\workspace";
libname  stateot "P:\MCD-SPVR\data\raw_data\SAS_DATASETS\OT";

/*macro to pull cdps claims for the other files, which are not split up by state*/
%macro pull_ot_claims(in_max,outdata);
	/*get cpds claims from file*/
	proc sql;
		create table otclaims_&outdata. as
		select *
		from &in_max. 
		where MAX_TOS in (51,52,53,54) ;
	quit;
%mend;

%macro do_states();
	%do i=1 %to 56;
	%if &i. ne 3 and 
			&i. ne 7 and 
			&i. ne 14 and 
			&i. ne 43 and
			&i. ne 52 and
			&i. ne 6 and
			&i. ne 36 and 
			&i. ne 48 %then %do;
			/*FIPS codes 3,7,14, 42, and 52 do not exist; skip CA, NY, and TX because the file name structure is difference*/
				%let state = %sysfunc(fipstate(&i));
				%pull_ot_claims(in_max= stateot.Maxdata_&state._ot_2012,outdata=&state.);
		%end;
	%end;	
%mend;
%do_states();

/*CA, NY, TX did not run in above loop because of file naming
/*run these manually below*/
%pull_ot_claims(in_max= stateot.Maxdata_ca_ot_2012_001,outdata=ca1);
%pull_ot_claims(in_max= stateot.Maxdata_ca_ot_2012_002,outdata=ca2);
%pull_ot_claims(in_max= stateot.Maxdata_ny_ot_2012_001,outdata=ny1);
%pull_ot_claims(in_max= stateot.Maxdata_ny_ot_2012_002,outdata=ny2);
%pull_ot_claims(in_max= stateot.Maxdata_tx_ot_2012_001,outdata=tx1);
%pull_ot_claims(in_max= stateot.Maxdata_tx_ot_2012_002,outdata=tx2);

data otclaims_ca;
	set otclaims_ca:;
run;
data otclaims_ny;
	set otclaims_ny:;
run;
data otclaims_tx;
	set otclaims_tx1 otclaims_tx2;
run;

proc sql;
	drop table otclaims_ca1, otclaims_ca2, otclaims_ny1, otclaims_ny2, otclaims_tx1, otclaims_tx2;
quit;
title;
data otclaims_all;
	set otclaims_:;
run;


ods excel file=" P:\MCD-SPVR\reports\tos_freqs.xlsx";
ods excel options(sheet_name="All" sheet_interval="none");
	proc freq data=otclaims_all;
		table max_tos*msis_tos/ missing nocum nofreq norow nocol;
	run;

ods excel options(sheet_interval="table"); ods exclude all; data _null_; file print; put _all_; run; ods select all;
%macro do_states();
	%do i=1 %to 56;
	%if &i. ne 3 and 
			&i. ne 7 and 
			&i. ne 14 and 
			&i. ne 43 and
			&i. ne 52 %then %do;
			/*FIPS codes 3,7,14, 42, and 52 do not exist; skip CA, NY, and TX because the file name structure is difference*/
				%let state = %sysfunc(fipstate(&i));

		ods excel options(sheet_name="&state." sheet_interval="none");
		proc odstext;
			p "&state. frequencies"/style=[color=blue font_weight=bold] ;
		run;
		proc freq data=otclaims_&state.;
			table max_tos*msis_tos/ missing nocum nofreq norow nocol out=freq_&state.;
		run;
		ods excel options(sheet_interval="table"); ods exclude all; data _null_; file print; put _all_; run; ods select all;
		%end;
	%end;	
%mend;
%do_states();
ods excel close;
proc contents data=freq_wy;run;

ods excel file=" P:\MCD-SPVR\reports\tos_matches.xlsx";
%macro do_states();
	%do i=1 %to 56;
	%if &i. ne 3 and 
			&i. ne 7 and 
			&i. ne 14 and 
			&i. ne 43 and
			&i. ne 52 %then %do;
			/*FIPS codes 3,7,14, 42, and 52 do not exist; skip CA, NY, and TX because the file name structure is difference*/
				%let state = %sysfunc(fipstate(&i));
		ods excel options(sheet_name="matches" sheet_interval="none");
		proc odstext;
			p "&state. frequencies"/style=[color=blue font_weight=bold] ;
		run;
		proc sql;
			select max_tos, msis_tos, count/sum(count) as percent
			from freq_&state.
			group by max_tos
			having percent > 0.9;
		quit;
		%end;
	%end;	
%mend;
%do_states();
ods excel close;

