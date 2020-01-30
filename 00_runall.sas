/*libraries*/
libname data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;
options obs=MAX;

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

proc contents data=space.pop_cdps_scores;run;

proc sql;
	create table out.msa_2012_29jan20 as
	select *
	from out.msa_2012 (drop=PREM_MDCD_PYMT_AMT_: FFS_PYMT_AMT:);
quit;

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

/*check cdps subpopulation files*/
proc sql;
	title "pula";
	select sum(dist_benes), sum(cell_n)
	from space.temp_pula;
	title "psyl";
	select sum(dist_benes), sum(cell_n)
	from space.temp_psyl;
	title "dia2l";
		select sum(dist_benes), sum(cell_n)
	from space.temp_dia2l;
	title "prgcmp";
		select sum(dist_benes), sum(cell_n)
	from space.temp_prgcmp;
	title "carel";
		select sum(dist_benes), sum(cell_n)
	from space.temp_carel;
	title "canl";
		select sum(dist_benes), sum(cell_n)
	from space.temp_canl;
	title;
quit;

proc sql;
	create table out.cdps_dia2l (drop= sum_comorbid partial_benf_mon rename=(dist_benes=cell_n sum_comorbid_fx=sum_comorbid partial_benf_mon_fx=partial_benf_mon)) as
	select *,
		case when partial_benf_mon is null then 0 else partial_benf_mon end as partial_benf_mon_fx label="Number partial benefit months",
		case when sum_comorbid is null then 0 else sum_comorbid end as sum_comorbid_fx label="Number beneficiaries with any of the 6 comorbid conditions"
	from space.temp_dia2l (drop=cell_n);

	create table out.cdps_pula (drop= sum_comorbid partial_benf_mon rename=(dist_benes=cell_n sum_comorbid_fx=sum_comorbid partial_benf_mon_fx=partial_benf_mon)) as
	select *,
		case when partial_benf_mon is null then 0 else partial_benf_mon end as partial_benf_mon_fx label="Number partial benefit months",
		case when sum_comorbid is null then 0 else sum_comorbid end as sum_comorbid_fx label="Number beneficiaries with any of the 6 comorbid conditions"
	from space.temp_pula (drop=cell_n); 

	create table out.cdps_psyl (drop= sum_comorbid partial_benf_mon rename=(dist_benes=cell_n sum_comorbid_fx=sum_comorbid partial_benf_mon_fx=partial_benf_mon)) as
	select *,
		case when partial_benf_mon is null then 0 else partial_benf_mon end as partial_benf_mon_fx label="Number partial benefit months",
		case when sum_comorbid is null then 0 else sum_comorbid end as sum_comorbid_fx label="Number beneficiaries with any of the 6 comorbid conditions"
	from space.temp_psyl (drop=cell_n);

	create table out.cdps_prgcmp (drop= sum_comorbid partial_benf_mon rename=(dist_benes=cell_n sum_comorbid_fx=sum_comorbid partial_benf_mon_fx=partial_benf_mon)) as
	select *,
		case when partial_benf_mon is null then 0 else partial_benf_mon end as partial_benf_mon_fx label="Number partial benefit months",
		case when sum_comorbid is null then 0 else sum_comorbid end as sum_comorbid_fx label="Number beneficiaries with any of the 6 comorbid conditions"
	from space.temp_prgcmp (drop=cell_n);

	create table out.cdps_carel (drop= sum_comorbid partial_benf_mon rename=(dist_benes=cell_n sum_comorbid_fx=sum_comorbid partial_benf_mon_fx=partial_benf_mon)) as
	select *,
		case when partial_benf_mon is null then 0 else partial_benf_mon end as partial_benf_mon_fx label="Number partial benefit months",
		case when sum_comorbid is null then 0 else sum_comorbid end as sum_comorbid_fx label="Number beneficiaries with any of the 6 comorbid conditions"
	from space.temp_carel (drop=cell_n);

	create table out.cdps_canl (drop= sum_comorbid partial_benf_mon rename=(dist_benes=cell_n sum_comorbid_fx=sum_comorbid partial_benf_mon_fx=partial_benf_mon)) as
	select *,
		case when partial_benf_mon is null then 0 else partial_benf_mon end as partial_benf_mon_fx,
		case when sum_comorbid is null then 0 else sum_comorbid end as sum_comorbid_fx
	from space.temp_canl (drop=cell_n);
quit;
proc means data=out.cdps_pula n mean std min max nmiss ;run;

proc freq data=space.temp_carel;
	tables st_msa*cell_n/list missing;
run;
proc sql;
	select sum(cell_n) 
	from out.cdps_dia2l;
quit;
proc sql;
	select st_msa, 
		sum(_0,_1_5,_6_18,_19_44,_45_64,_65_84,_85p) as age_sum,
		sum(boe_adult,boe_aged, boe_child, boe_disabled,boe_fchild,boe_uadult,boe_uchild) as boe_sum,
		sum(mas_1115,mas_cash,mas_mn,mas_oth,mas_pov) as mas_sum
	from out.cdps_canl
	group by st_msa
	having age_sum > 1 or boe_sum > 1 or mas_sum > 1;
quit;

proc freq data=temp;
	tables age_cell*st_msa/list missing;
run;
proc means data=space.temp_dia2l n mean std min max nmiss ;
	where age_cell = "adult_05";
run;
proc contents data=space.temp_dia2l;
run;
proc contents data=out.msa_2012_02nov2019;
run;
proc freq data=out.cdps_dia2l;
	tables st_msa*partial_benf_mon*cell_n/list missing;
run;
proc sql;
	select sum(cell_n)
	from out.cdps_canl;
quit;

proc sql;
	create table dia2l_adu as
	select *
	from space.temp_dia2l
	where substr(age_cell,1,3) = "adu";

	create table dia2l_chi as
	select *
	from space.temp_dia2l
	where substr(age_cell,1,3) = "chi";
quit;

proc freq data=space.temp_dia2l;
	tables _0 _1_5 _6_18 _19_44 _45_64 _65_84 _85p/list missing;
run;
