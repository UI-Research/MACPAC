* Date for version control;
%let date=10_30_18;
options obs=MAX;
* log;
PROC PRINTTO PRINT="P:\MCD-SPVR\log\CDPS_score_v1030_OR_&sysdate..lst"
               LOG="P:\MCD-SPVR\log\CDPS_score_v1030_OR_&sysdate..log" NEW;
RUN;

libname  data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname  space   "P:\MCD-SPVR\data\workspace";
libname  area    "P:\MCD-SPVR\data\NO_PII";
libname  out     "P:\MCD-SPVR\data\workspace\output" COMPRESS=YES;
libname  library "P:\MCD-SPVR\data\workspace\output";
libname  stateot "P:\MCD-SPVR\data\raw_data\SAS_DATASETS\OT";
libname cpds_wt  "P:\MCD-SPVR\data\NO_PII\CDPS_WEIGHTS";
libname  scores  "P:\MCD-SPVR\data\workspace\CDPS_SCORES";
filename cdpsprog 'P:\MCD-SPVR\programs\CDPS_5.4\cdps01.sas';
%include cdpsprog;

%macro run_cdps_aid(state, aid);

options spool;

%let vars = a_under1 a_1_4 a_5_14m 	a_5_14f	a_15_24m a_15_24f a_25_44m	a_25_44f a_45_64m a_45_64f a_65
			AIDSH INFH HIVM INFM INFL CANVH CANH CANM CANL CARVH CARM CARL CAREL
			CERL CNSH CNSM CNSL DIA1H DIA1M DIA2M DIA2L DDM DDL EYEL EYEVL 
            		GENEL GIH GIM GIL HEMEH HEMVH HEMM HEML METH METM METVL PRGCMP PRGINC
			PSYH PSYM PSYML PSYL SUBL SUBVL PULVH PULH PULM PULL RENEH RENVH 
			RENM RENL SKCM SKCL SKCVL SKNH SKNL SKNVL;

%let var_cnt=%sysfunc(countw(&vars.));



data inelig (keep=recipno age male adult category);
	set space.categories_full_2012 (keep=BENE_ID STATE_CD EL_DOB male age_cat cell);
	%if &aid=DA %then %do; 
	where STATE_CD="&state" and ((age_cat=2 and cell in('1','2','5','6','9','10','11','12','13','14','15','16')) or 
					(age_cat=3 and cell in('1','2','5','6','9','10','11','12','13','14','15','16')));
	category="DA";
	%end;
	%else %if &aid=DC %then %do; 
	where STATE_CD="&state" and age_cat=1 and cell in('1','2','5','6','9','10','11','12','13','14','15','16');
	category="DC";
	%end;
	%else %if &aid=AA %then %do; 
	where STATE_CD="&state" and age_cat=2 and cell in('3','4','7','8');
	category="AA";
	%end;
	%else %if &aid=AC %then %do; 
	where STATE_CD="&state" and age_cat=1 and cell in('3','4','7','8','17');
	category="AC";
	%end;
	
	if missing(BENE_ID) then DELETE;
	recipno=catx ('_',STATE_CD,BENE_ID);
	
	if not missing(EL_DOB) then do;
		calcdate = mdy(12,1,2012);
				format calcdate mmddyy8.;
				age = INT(INTCK('MONTH',EL_DOB,calcdate)/12);
				IF MONTH(EL_DOB) = MONTH(calcdate) THEN age = age-(DAY(EL_DOB)>DAY(calcdate));
				end;
run;

proc sort data=inelig nodupkeys;
	by recipno;
run;
                                                 
%cdps(&aid);


proc import 
	dbms=excel out=scores_temp datafile="P:\MCD-SPVR\data\NO_PII\CDPS_WEIGHTS\CDPS52_CON.XLSX";
	sheet="&aid."; getnames=yes; run;
data scores;
  set scores_temp;
	_type_="PARMS";
	_model_="CDPS_SCORE";
run;

proc score data=step2_&aid. score=scores out=score_&aid. type=parms;
run;

title "Summary of CDPS Dx Categories and score for State: &state , aid category: &aid";

proc summary data=step2_&aid.;
var %names;
output out=see sum=;

proc print data=see noobs;
run;

proc means data=score_&aid.;
var nocdps CDPS_SCORE;
run;
title " ";

%mend run_cdps_aid;

%macro run_cdps(state);
data otdxs;
    		set stateot.Maxdata_&state._ot_2012 (keep=BENE_ID STATE_CD DIAG_CD_1 DIAG_CD_2);
    		
	diag1=DIAG_CD_1;
	diag2=DIAG_CD_2;
	if missing(BENE_ID) then DELETE;
	recipno=catx ('_',STATE_CD,BENE_ID);
    keep recipno diag1 diag2;
run;

data ipdxs (keep=recipno diag1 diag2);
set data.maxdata_ip_2012 (keep=BENE_ID STATE_CD DIAG_CD_1-DIAG_CD_9);
	where STATE_CD="&state.";
	if missing(BENE_ID) then DELETE;
	recipno=catx ('_',STATE_CD,BENE_ID);
	diag1=DIAG_CD_1;
	diag2=DIAG_CD_2;
	output ipdxs;
	if not missing(DIAG_CD_3) then do;
		diag1=DIAG_CD_3;
		diag2=DIAG_CD_4;
		output ipdxs;
		if not missing(DIAG_CD_5) then do;
			diag1=DIAG_CD_5;
			diag2=DIAG_CD_6;
			output ipdxs;
			if not missing(DIAG_CD_7) then do;
				diag1=DIAG_CD_7;
				diag2=DIAG_CD_8;
				output ipdxs;
				if not missing(DIAG_CD_9) then do;
					diag1=DIAG_CD_9;
					output ipdxs;
					end;
				end;
			end;
		end;
run;

data step1;
set otdxs ipdxs;
run;

proc sort data=step1;
	by recipno;
run;

%run_cdps_aid(&state.,DA);
%run_cdps_aid(&state.,DC);
%run_cdps_aid(&state.,AA);
%run_cdps_aid(&state.,AC);


data scores.cdps_&state;
	set score_DA score_DC score_AA score_AC;
run;

%mend run_cdps;

*BEGIN PROCESSING;

%run_cdps (OR);

run;



