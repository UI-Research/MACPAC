/*libraries*/
libname data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;
options obs=100000;

/*Step 1:
%let indata=data.maxdata_ps_2012; 
%let outdata=space.temp_personlevel;
*/
%include "P:\MCD-SPVR\programs\02_Assemble_Data_Files\sandbox\01_popfile.sas";
%put "01 done";
/*Step 2:
%let indata= space.temp_personlevel;
%let outdata=space.temp_personlevel_geofixed;
*/
%include "P:\MCD-SPVR\programs\02_Assemble_Data_Files\sandbox\02_geofix.sas";
%put "02 done";
/*Step 3:
%let indata=space.temp_personlevel_geofixed;
%let personlevel=space.temp_personlevel;
also produced: 
space.id_pop
space.id_pop_dropped
*/
%include "P:\MCD-SPVR\programs\02_Assemble_Data_Files\sandbox\03_popfile_finaltables.sas";
%put "03 done";
/*Step 4 is the CDPS calculations*/

/*Step 5:
%let indata_max = space.id_pop;
%let outdata=space.pop_cdps_scores; UPDATE THIS TABLE NAME FORWARD
Also produced: 
space.temp_ahrf_msa_xwalk
space.temp_ahrf_aggre 
*/
%include "P:\MCD-SPVR\programs\02_Assemble_Data_Files\sandbox\05_addcdps.sas";
%put "05 done";
/*Step 6: 
%let indata=space.temp_max_cdpsscores; 
%let outdata= space.temp_msa_arhfvars_wageind;
*/
%include "P:\MCD-SPVR\programs\02_Assemble_Data_Files\sandbox\06_collapsemsa.sas";
%put "06 done";
%include "P:\MCD-SPVR\programs\02_Assemble_Data_Files\sandbox\07_addstats.sas";
%put "07 done";
%include "P:\MCD-SPVR\programs\02_Assemble_Data_Files\sandbox\08_finalreports.sas";

/*Start CDPS chronic conditions population*/
/*Separated out CDPS chronic conditions populations*/
%include "P:\MCD-SPVR\programs\02_Assemble_Data_Files\sandbox\09_cdps_pops.sas";
%put "09 done";

/*get total CDPS population counts*/
proc freq data=space.pop_cdps_scores;
	tables dia2l*pula*carel*canl*psyl*prgcmp/list missing;
run;
options obs=MAX;
proc sql;
	select 
		sum(prgcmp) as prgcmp_count,
		sum(case when prgcmp=1 and (pula=1 or carel=1 or canl=1 or psyl=1 or dia2l=1) then 1 else 0 end) as prgcmp_comorbid_count
	from space.pop_cdps_scores
	where prgcmp=1 and 
		substr(age_cell,length(age_cell)-1,2) in ("05","06","07","08") and 
		age_cat in (1,2) and
		EL_RSTRCT_BNFT_FLG_LTST in ("1","7","8","A","B");
quit;


proc sql;
	select count(*) as all_count,
		sum(dia2l) as dia2l_count,
		sum(case when dia2l=1 and (pula=1 or carel=1 or canl=1 or psyl=1 or prgcmp=1) then 1 else 0 end) as dia2l_comorbid_count,
		sum(pula) as pula_count,
		sum(case when pula=1 and (dia2l=1 or carel=1 or canl=1 or psyl=1 or prgcmp=1) then 1 else 0 end) as pula_comorbid_count,
		sum(carel) as carel_count,
		sum(case when carel=1 and (pula=1 or dia2l=1 or canl=1 or psyl=1 or prgcmp=1) then 1 else 0 end) as carel_comorbid_count,
		sum(canl) as canl_count,
		sum(case when canl=1 and (pula=1 or carel=1 or dia2l=1 or psyl=1 or prgcmp=1) then 1 else 0 end) as canl_comorbid_count,
		sum(psyl) as psyl_count,
		sum(case when psyl=1 and (pula=1 or carel=1 or canl=1 or dia2l=1 or prgcmp=1) then 1 else 0 end) as psyl_comorbid_count,
		sum(prgcmp) as prgcmp_count,
		sum(case when prgcmp=1 and (pula=1 or carel=1 or canl=1 or psyl=1 or dia2l=1) then 1 else 0 end) as prgcmp_comorbid_count
	from space.pop_cdps_scores
	where substr(age_cell,length(age_cell)-1,2) in ("05","06","07","08") and 
		age_cat in (1,2) and
		EL_RSTRCT_BNFT_FLG_LTST in ("1","7","8","A","B");
quit;
