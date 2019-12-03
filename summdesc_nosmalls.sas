/*******************************************************************************************************************/ 
/*	Purpose: Summarize and Describe Condition output		
/*	Project: MACPAC Spending Variations
/*	Author: Tim Waidmann
/*	Notes: 
/*******************************************************************************************************************/ 
/*Options to change*/
%macro prod();
	options obs=MAX cleanup;
	/*Log*/
	/*proc printto print="P:\MCD-SPVR\log\15_cdps_msalevel_reports&sysdate..lst"
	               log="P:\MCD-SPVR\log\15_cdps_msalevel_reports&sysdate..log" NEW;
	run;*/
%mend prod;

%macro test();	
	options obs=100000;
	/*Log*/
	proc printto;run;
%mend;

%prod();
*%test();

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

/*macro vars*/
%let year=2012;


%macro summdesc(dxname);
title "&dxname";
title2 "File Summary";
run;
proc means data=space.&dxname._MSA_Summary_nosmall stackodsoutput maxdec=2 n mean stddev min p50 p75 p95 max ;
vars bene_count dual_mon--nf_clm_dx_p99;
weight bene_count;
run;
run;
%mend;

%summdesc(asthma);
%summdesc(hypertension);
%summdesc(diabetes);
%summdesc(pregnancy);
%summdesc(mntl_hlth);
%summdesc(cancer);
run;

