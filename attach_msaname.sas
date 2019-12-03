/*******************************************************************************************************************/ 
/*	Purpose: Attach MSA name to files		
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

/*get benes that are ffs, non-dual, non-seniors, with cdps flags of interest*/
*add ahrf/msa data to claims;
data st_msa (keep=state_cd cbsacode cbsatitle);
set space.ahrf_msa;
where metropolitanmicropolitanstatis= 'Metropolitan Statistical Area';
run;
proc sort data=st_msa out=st_msa_list nodupkey; by state_cd cbsacode;
run;
%macro addname(cdps_word, dxname);
proc sql;
	create table work.&dxname._MSA_Summary as
	select B.cbsatitle, B.state_cd, A.* 
	from space.&cdps_word._collapsed A left join st_msa_list B
	on substr(A.st_msa,4,5) = B.cbsacode and substr(A.st_msa,1,2)=B.state_cd;
quit;
run;
data space.&dxname._MSA_Summary;
set work.&dxname._MSA_Summary;
if substr(st_msa,4,5)="XXXXX" then do; 
	cbsatitle="_NonMetro";
	state_cd=substr(st_msa,1,2);
	end;
label sum_dx_Pspend_i="Sum of Individual Predicted Spending for diagnosis related claims"; 
run;
proc sort data=space.&dxname._MSA_Summary; 
	by state_cd cbsatitle; 
run;

data space.&dxname._MSA_Summary_nosmall; 
	set space.&dxname._MSA_Summary;
	if bene_count >=100;
	run;

%mend;
%macro summdesc(dxname);
title "&dxname";
title2 "File Summary";
run;
proc means data=space.&dxname._MSA_Summary_nosmall stackodsoutput maxdec=2 n mean stddev min p50 p75 p95 max ;
vars bene_count dual_mon--mean_dx_rx_spd_dxTC;
weight bene_count;
run;
%mend;

run;

%addname(asthma,asthma);
%addname(hypertension, hypertension);
%addname(diabetes,diabetes);
%addname(pregcomp, pregnancy);
%addname(psych, mntl_hlth);
%addname(cancers, cancer);
%summdesc(asthma);
%summdesc(hypertension);
%summdesc(diabetes);
%summdesc(pregnancy);
%summdesc(mntl_hlth);
%summdesc(cancer);
run;

