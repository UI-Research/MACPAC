/*******************************************************************************************************************/ 
/*	Purpose: Create MSA-level CDPS flag population analytic files		
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak
/*	Notes: 
/*		1) Collapse macros for easier manipulation
/*******************************************************************************************************************/ 
/*Options to change*/
%macro prod();
	options obs=MAX cleanup;
	/*Log*/
	proc printto;
	run;
%mend;

%macro test();	
	options obs=1000000;
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

%let indata_max = space.max2012_cdps_subset; /*input data file from 05_subset_claims_byDX*/
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


/* cdps flags of interest
dia2l
CAREL /*hypertension*
CANL /*colon, prostate, cervical cancer*
PRGCMP /*completed pregcancy*
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
/*bring in cdps data + max data (created in 03_aggregate program), limited to just the CDPS flags and age_servicetype groups of interest*/

/********************************************************/
/*Initial processing to attach MSA info to files*/
/********************************************************/
%macro msa(in_data);
	proc sql;
		create table ahrf_msa_xwalk as
		select *,
			&year. as year,
			catx("-",state_cd,county_fips) as st_cnty,
			case when state_fips = '72' then 'PR' 
				else state_cd 
				end as state_cd_fx,
			case when metropolitanmicropolitanstatis = 'Metropolitan Statistical Area' then catx('-', state_cd, cbsacode)
				else catx('-', state_cd, "XXXXX")
				end as st_msa,
			case when missing(unemp_d) or missing(unemp_n) then 1 else 0 end as ahrf_msg
		from space.ahrf_msa;
		
		create table ahrf_aggre as
		select year, st_msa,
			1000*sum(hos_n)/sum(pop)label = "Number of hospital beds per 1k people, 2010" as beds, 
			1000*sum(md_n)/sum(pop)label = "Number of physicians per 1k people, 2010" as md, 
			sum(poverty_d)/sum(poverty_n) label = "Rate of persons in poverty" as povrate,
			sum(unemp_d)/sum(unemp_n) label = "Unemployment rate" as urate,
			sum(ahrf_msg) as sum_ahrf_msg
		from ahrf_msa_xwalk
		group by year, st_msa;
	quit;

	proc sql ;
		create table max_2012_msa_join as
		select  a.*, b.* /*,cbsatitle_fx as cbsatitle*/
		from &in_data. a left join ahrf_msa_xwalk (drop=year) b
		on a.county=b.st_cnty;
	quit;

	proc freq data=max_2012_msa_join;
		title "Obs with ST_MSA matches";
		tables st_msa county/list missing;
		format st_msa county $missing_char.;
	run;

	proc sql;
		title Obs without ST_MSA matches;
		select county, count(county) as number_missing
		from max_2012_msa_join
		where st_msa = ' '
		group by county;
		title;
	quit;
	
	proc sql;
		create table max_msa_2012 as
		select *
		from max_2012_msa_join
		where st_msa ne ' ';
	quit;
%mend;
%msa(in_data=&indata_max.);

/***************************/
/*Collapse to specified var*/
/***************************/
%macro collapse (indata,collapse_on,outdata,cdpsflag);
	proc sql;
		create table &outdata. as
			select year, &collapse_on., 
			sum(TOT_MDCD_PYMT_AMT) as spending,
			sum(FFS_PYMT_AMT_01) as ffspymt_01,
			sum(phys_clin_spending) as physclin, 
			sum(FFS_PYMT_AMT_16) as ffspymt_16,
			sum(mo_dual) as dual_mon, 
			sum(mo_mc) as mc_mon, 
			sum(mo_dsbl) as dis_mon, 
			sum(mo_ltss) as ltss_mon,
			sum(EL_ELGBLTY_MO_CNT) as elg_mon, 
			sum(cell_n) as cell_n,
			sum(d_servicetype_n) as d_servicetype_n, 
			sum(died_n) as died_n,
			sum(mas_cash) as mas_cash_n, 
			sum(mas_cash)/sum(cell_n) as mas_cash,
			sum(mas_mn) as mas_mn_n, 
			sum(mas_mn)/sum(cell_n) as mas_mn, 
			sum(mas_pov) as mas_pov_n, 
			sum(mas_pov)/sum(cell_n) as mas_pov, 
			sum(mas_1115) as mas_1115_n,
			sum(mas_1115)/sum(cell_n) as mas_1115,
			sum(mas_oth) as mas_oth_n, 
			sum(mas_oth)/sum(cell_n) as mas_oth, 
			sum(boe_aged) as boe_aged_n, 
			sum(boe_aged)/sum(cell_n) as boe_aged, 
			sum(boe_disabled) as boe_disabled_n, 
			sum(boe_disabled)/sum(cell_n) as boe_disabled, 
			sum(boe_child) as boe_child_n, 
			sum(boe_child)/sum(cell_n) as boe_child, 
			sum(boe_adult) as boe_adult_n, 
			sum(boe_adult)/sum(cell_n) as boe_adult, 
			sum(boe_uchild) as boe_uchild_n, 
			sum(boe_uchild)/sum(cell_n) as boe_uchild, 
			sum(boe_uadult) as boe_uadult_n, 
			sum(boe_uadult)/sum(cell_n) as boe_uadult, 
			sum(boe_fchild) as boe_fchild_n,
			sum(boe_fchild)/sum(cell_n) as boe_fchild,
			sum(male) as male_n, 
			sum(male)/sum(cell_n) as male, 
			sum(age_0) as _0_n, 
			sum(age_0)/sum(cell_n) as _0, 
			sum(age_1_5) as _1_5_n, 
			sum(age_1_5)/sum(cell_n) as _1_5, 
			sum(age_6_18) as _6_18_n, 
			sum(age_6_18)/sum(cell_n) as _6_18, 
			sum(age_19_44) as _19_44_n, 
			sum(age_19_44)/sum(cell_n) as _19_44, 
			sum(age_45_64) as _45_64_n, 
			sum(age_45_64)/sum(cell_n) as _45_64, 
			sum(age_65_84) as _65_84_n, 
			sum(age_65_84)/sum(cell_n) as _65_84, 
			sum(age_85p) as _85p_n, 
			sum(age_85p)/sum(cell_n) as _85p,
			mean(CDPS_SCORE) as cdps,
			mean(NOCDPS) as no_cdps_conds
			/*mean(pspend_i) as pred_mcd_spd*/
			from &indata. 
			where &cdpsflag=1
			group by year, &collapse_on.;
		quit;
%mend;

%collapse(indata=max_msa_2012,collapse_on=%str(age_cat, age_servicetype, st_msa,dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_dia2l, cdpsflag=dia2l);
%collapse(indata=max_msa_2012,collapse_on=%str(age_cat, age_servicetype, st_msa,dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_carel, cdpsflag=carel);
%collapse(indata=max_msa_2012,collapse_on=%str(age_cat, age_servicetype, st_msa,dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_canl, cdpsflag=canl);
%collapse(indata=max_msa_2012,collapse_on=%str(age_cat, age_servicetype, st_msa,dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_prgcmp, cdpsflag=prgcmp);
%collapse(indata=max_msa_2012,collapse_on=%str(age_cat, age_servicetype, st_msa,dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_psyl, cdpsflag=psyl);
%collapse(indata=max_msa_2012,collapse_on=%str(age_cat, age_servicetype, st_msa,dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_pula, cdpsflag=pula);

%collapse(indata=max_msa_2012,collapse_on=%str(age_cat, st_msa),outdata=msa_dia2l_nomasboe, cdpsflag=dia2l);
%collapse(indata=max_msa_2012,collapse_on=%str(age_cat, st_msa),outdata=msa_carel_nomasboe, cdpsflag=carel);
%collapse(indata=max_msa_2012,collapse_on=%str(age_cat, st_msa),outdata=msa_canl_nomasboe, cdpsflag=canl);
%collapse(indata=max_msa_2012,collapse_on=%str(age_cat, st_msa),outdata=msa_prgcmp_nomasboe, cdpsflag=prgcmp);
%collapse(indata=max_msa_2012,collapse_on=%str(age_cat, st_msa),outdata=msa_psyl_nomasboe, cdpsflag=psyl);
%collapse(indata=max_msa_2012,collapse_on=%str(age_cat, st_msa),outdata=msa_pula_nomasboe, cdpsflag=pula);

%collapse(indata=max_msa_2012,collapse_on=%str(st_msa, dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_dia2l_noage, cdpsflag=dia2l);
%collapse(indata=max_msa_2012,collapse_on=%str(st_msa, dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_carel_noage, cdpsflag=carel);
%collapse(indata=max_msa_2012,collapse_on=%str(st_msa, dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_canl_noage, cdpsflag=canl);
%collapse(indata=max_msa_2012,collapse_on=%str(st_msa, dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_prgcmp_noage, cdpsflag=prgcmp);
%collapse(indata=max_msa_2012,collapse_on=%str(st_msa, dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_psyl_noage, cdpsflag=psyl);
%collapse(indata=max_msa_2012,collapse_on=%str(st_msa, dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_pula_noage, cdpsflag=pula);

%collapse(indata=max_msa_2012,collapse_on=st_msa,outdata=msa_dia2l_nomasboe_noage, cdpsflag=dia2l);
%collapse(indata=max_msa_2012,collapse_on=st_msa,outdata=msa_carel_nomasboe_noage, cdpsflag=carel);
%collapse(indata=max_msa_2012,collapse_on=st_msa,outdata=msa_canl_nomasboe_noage, cdpsflag=canl);
%collapse(indata=max_msa_2012,collapse_on=st_msa,outdata=msa_prgcmp_nomasboe_noage, cdpsflag=prgcmp);
%collapse(indata=max_msa_2012,collapse_on=st_msa,outdata=msa_psyl_nomasboe_noage, cdpsflag=psyl);
%collapse(indata=max_msa_2012,collapse_on=st_msa,outdata=msa_pula_nomasboe_noage, cdpsflag=pula);

%macro sum_tab(cdps_msa_dat,variable,largecelln,outdata);
%let cdps_msa_dat=msa_dia2l;
%let variable=cell_n; %let largecelln=100; %let outdata=dia2l_n_tab_nomasboe_noage;
	proc sql;
		create table age_servicetypes as 
		select distinct(age_servicetype)
		from &cdps_msa_dat.;

		create table unique_msas as
		select distinct st_msa
		from ahrf_msa_xwalk;

		create table msas as
		select *, 0 as &variable.,
			case when substr(age_servicetype,1,3) = "chi" then 1
				when substr(age_servicetype,1,3) = "adu" then 2
				when substr(age_servicetype,1,3) = "sen" then 3
				else .
			end as age_cat 
		from unique_msas, age_servicetypes;

		create table ageservicetypemsas as
		select *, 
			coalesce(a.st_msa,b.st_msa) as new_st_msa, 
			coalesce(a.age_servicetype,b.age_servicetype) as new_age_servicetype,
			coalesce(a.&variable.,b.&variable.) as new_&variable.,
			coalesce(a.age_cat,b.age_cat) as new_age_cat
		from &cdps_msa_dat. a full join msas b
		on a.st_msa = b.st_msa and a.age_servicetype=b.age_servicetype;
	quit;

	proc univariate data=ageservicetypemsas noprint;
		class new_age_servicetype;
		var new_&variable.;
		output out=n_pctls
		pctlpts = 1 5 10 25 50 75 90 95 99
		pctlpre=&variable._p
		mean=new_&variable._avg
		min=new_&variable._min
		max=new_&variable._max
		sum=sum_benes;
	run;

	proc sql;
		create table all_cells_count as
		select * from
			(select new_age_servicetype, count(*) as cell_count
			from ageservicetypemsas
			group by new_age_servicetype) a 
		inner join  
			(select new_age_servicetype, count(new_&variable.) as big_cell_count
			from ageservicetypemsas
			where new_cell_n>=&largecelln.
			group by new_age_servicetype) b
		on a.new_age_servicetype = b.new_age_servicetype
		inner join
			(select new_age_servicetype, count(distinct new_st_msa) as n_st_msas
			from ageservicetypemsas
			group by new_age_servicetype) c
		on a.new_age_servicetype = c.new_age_servicetype;

		create table big_cell_perc as
		select a.*, b.cell_count, b.big_cell_count, b.big_cell_count/b.cell_count as perc_big_cells, b.n_st_msas,
			case when substr(a.new_age_servicetype,1,3)="chi" then 1
				when substr(a.new_age_servicetype,1,3)="adu" then 2
				when substr(a.new_age_servicetype,1,3)="sen" then 3
			end as agecat
		from n_pctls a left join all_cells_count b 
		on a.new_age_servicetype=b.new_age_servicetype;
	quit;
	proc print data=big_cell_perc;run;

	proc sql;
		create table &outdata. as
		select agecat, new_age_servicetype,
			new_&variable._avg as avg label="Average Benes By MSA" format=COMMA8., 
			&variable._p1 as p1 label="p1", 
			&variable._p5 as p5 label="p5" format=COMMA8., 
			&variable._p10 as p10 label="p10" format=COMMA8., 
			&variable._p25 as p25 label="p25" format=COMMA8., 
			&variable._p50 as p50 label="p50" format=COMMA8., 
			&variable._p75 as p75 label="p75" format=COMMA8., 
			&variable._p90 as p90 label="p90" format=COMMA8., 
			&variable._p95 as p95 label="p95" format=COMMA8., 
			&variable._p99 as p99 label="p99" format=COMMA8., 
			new_&variable._min as min label="Min Benes By MSA" format=COMMA8., 
			new_&variable._max as max label="Max Benes By MSA" format=COMMA8., 
			n_st_msas label="St-MSA N" format=COMMA8.,
			sum_benes label="Total Benes" format=COMMA8.,
			big_cell_count/cell_count as pct_lg_msas label="Percent Large MSAs*" format=PERCENT8.2
		from big_cell_perc;
	quit;
%mend;

%macro sum_tab_nomasboe(cdps_msa_dat,variable,largecelln,outdata);
	proc sql;
		create table age_cats as 
		select distinct(age_cat) as age_cat
		from &cdps_msa_dat.;

		create table unique_msas as
		select distinct st_msa
		from ahrf_msa_xwalk;

		create table msas as
		select *, 0 as &variable.
		from unique_msas, age_cats;

		create table agecats_msas as
		select *, b.st_msa as bstmsa, b.age_cat as bagecat,
			coalesce(a.st_msa, b.st_msa) as new_st_msa, 
			coalesce(a.&variable., b.&variable.) as new_&variable.,
			coalesce(a.age_cat, b.age_cat) as new_age_cat
		from &cdps_msa_dat. a full join msas b
		on a.st_msa = b.st_msa and a.age_cat=b.age_cat;
	quit;

	proc univariate data=agecats_msas noprint;
		class new_age_cat;
		var new_&variable.;
		output out=n_pctls
		pctlpts = 1 5 10 25 50 75 90 95 99
		pctlpre=&variable._p
		mean=new_&variable._avg
		min=new_&variable._min
		max=new_&variable._max
		sum=sum_benes;
	run;

	proc sql;
		create table all_cells_count as
		select * from
			(select new_age_cat, count(*) as cell_count
			from agecats_msas
			group by new_age_cat) a 
		inner join  
			(select new_age_cat, count(new_&variable.) as big_cell_count
			from agecats_msas
			where new_cell_n>=&largecelln.
			group by new_age_cat) b
		on a.new_age_cat = b.new_age_cat
		inner join
			(select new_age_cat, count(distinct new_st_msa) as n_st_msas
			from agecats_msas
			group by new_age_cat) c
		on a.new_age_cat = c.new_age_cat;

		create table big_cell_perc as
		select a.*, b.cell_count, b.big_cell_count, b.big_cell_count/b.cell_count as perc_big_cells, b.n_st_msas
		from n_pctls a left join all_cells_count b 
		on a.new_age_cat=b.new_age_cat;
	quit;

	proc sql;
		create table &outdata. as
		select new_age_cat,
			new_&variable._avg as avg label="Average Benes By MSA" format=COMMA8., 
			&variable._p1 as p1 label="p1", 
			&variable._p5 as p5 label="p5" format=COMMA8., 
			&variable._p10 as p10 label="p10" format=COMMA8., 
			&variable._p25 as p25 label="p25" format=COMMA8., 
			&variable._p50 as p50 label="p50" format=COMMA8., 
			&variable._p75 as p75 label="p75" format=COMMA8., 
			&variable._p90 as p90 label="p90" format=COMMA8., 
			&variable._p95 as p95 label="p95" format=COMMA8., 
			&variable._p99 as p99 label="p99" format=COMMA8., 
			new_&variable._min as min label="Min Benes By MSA" format=COMMA8., 
			new_&variable._max as max label="Max Benes By MSA" format=COMMA8., 
			n_st_msas label="St-MSA N" format=COMMA8.,
			sum_benes label="Total Benes" format=COMMA8.,
			big_cell_count/cell_count as pct_lg_msas label="Percent Large MSAs*" format=PERCENT8.2
		from big_cell_perc;
	quit;
%mend;

%macro sum_tab_noage(cdps_msa_dat,variable,largecelln,outdata);
	proc sql;
		create table servicetypes as 
		select distinct dis_cat, dual_cat, ltss_cat, mc_cat,
			case when dis_cat = 0 and dual_cat = 0 and ltss_cat = 0 and mc_cat = 0 then "05"
				when dis_cat = 0 and dual_cat = 0 and ltss_cat = 1 and mc_cat = 0 then "06"
				when dis_cat = 1 and dual_cat = 0 and ltss_cat = 0 and mc_cat = 0 then "07"
				when dis_cat = 1 and dual_cat = 0 and ltss_cat = 1 and mc_cat = 0 then "08"
			end as servicetype
		from &cdps_msa_dat.;

		create table unique_msas as
		select distinct st_msa
		from ahrf_msa_xwalk;

		create table msas as
		select *, 0 as &variable.
		from unique_msas, servicetypes;

		create table servicetypemsas as
		select *, 
			coalesce(a.st_msa,b.st_msa) as new_st_msa, 
			coalesce(a.servicetype,b.servicetype) as new_servicetype,
			coalesce(a.&variable.,b.&variable.) as new_&variable.
		from (select *, 
			case when dis_cat = 0 and dual_cat = 0 and ltss_cat = 0 and mc_cat = 0 then "05"
				when dis_cat = 0 and dual_cat = 0 and ltss_cat = 1 and mc_cat = 0 then "06"
				when dis_cat = 1 and dual_cat = 0 and ltss_cat = 0 and mc_cat = 0 then "07"
				when dis_cat = 1 and dual_cat = 0 and ltss_cat = 1 and mc_cat = 0 then "08"
			end as servicetype
			from &cdps_msa_dat.) a 
		full join msas b
		on a.st_msa = b.st_msa and a.servicetype=b.servicetype;
	quit;

	proc univariate data=servicetypemsas noprint;
		class new_servicetype;
		var new_&variable.;
		output out=n_pctls
		pctlpts = 1 5 10 25 50 75 90 95 99
		pctlpre=&variable._p
		mean=new_&variable._avg
		min=new_&variable._min
		max=new_&variable._max
		sum=sum_benes;
	run;

	proc sql;
		create table all_cells_count as
		select * from
			(select new_servicetype, count(*) as cell_count
			from servicetypemsas
			group by new_servicetype) a 
		inner join  
			(select new_servicetype, count(new_&variable.) as big_cell_count
			from servicetypemsas
			where new_cell_n>=&largecelln.
			group by new_servicetype) b
		on a.new_servicetype = b.new_servicetype
		inner join
			(select new_servicetype, count(distinct new_st_msa) as n_st_msas
			from servicetypemsas
			group by new_servicetype) c
		on a.new_servicetype = c.new_servicetype;

		create table big_cell_perc as
		select a.*, b.cell_count, b.big_cell_count, b.big_cell_count/b.cell_count as perc_big_cells, b.n_st_msas
		from n_pctls a left join all_cells_count b 
		on a.new_servicetype=b.new_servicetype;
	quit;

	proc sql;
		create table &outdata. as
		select new_servicetype,
			new_&variable._avg as avg label="Average Benes By MSA" format=COMMA8., 
			&variable._p1 as p1 label="p1", 
			&variable._p5 as p5 label="p5" format=COMMA8., 
			&variable._p10 as p10 label="p10" format=COMMA8., 
			&variable._p25 as p25 label="p25" format=COMMA8., 
			&variable._p50 as p50 label="p50" format=COMMA8., 
			&variable._p75 as p75 label="p75" format=COMMA8., 
			&variable._p90 as p90 label="p90" format=COMMA8., 
			&variable._p95 as p95 label="p95" format=COMMA8., 
			&variable._p99 as p99 label="p99" format=COMMA8., 
			new_&variable._min as min label="Min Benes By MSA" format=COMMA8., 
			new_&variable._max as max label="Max Benes By MSA" format=COMMA8., 
			n_st_msas label="St-MSA N" format=COMMA8.,
			sum_benes label="Total Benes" format=COMMA8.,
			big_cell_count/cell_count as pct_lg_msas label="Percent Large MSAs*" format=PERCENT8.2
		from big_cell_perc;
	quit;
%mend;

%macro sum_tab_nomasboe_noage(cdps_msa_dat,variable,largecelln,outdata);
	proc sql;
		create table unique_msas as
		select distinct st_msa, 0 as &variable.
		from ahrf_msa_xwalk;

		create table nomasboe_noage as
		select *, 
			coalesce(a.st_msa, b.st_msa) as new_st_msa, 
			coalesce(a.&variable.,b.&variable.) as new_&variable.
		from &cdps_msa_dat. a 
		full join unique_msas b
		on a.st_msa = b.st_msa;
	quit;

	proc univariate data=nomasboe_noage noprint;
		var new_&variable.;
		output out=n_pctls
		pctlpts = 1 5 10 25 50 75 90 95 99
		pctlpre=&variable._p
		mean=new_&variable._avg
		min=new_&variable._min
		max=new_&variable._max
		sum=sum_benes;
	run;

	proc sql;
		create table all_cells_count as
		select * from
			(select count(*) as cell_count
			from nomasboe_noage),  
			(select count(new_&variable.) as big_cell_count
			from nomasboe_noage
			where new_cell_n>=&largecelln.),
			(select count(distinct new_st_msa) as n_st_msas
			from nomasboe_noage);

		create table big_cell_perc as
		select a.*, b.cell_count, b.big_cell_count, b.big_cell_count/b.cell_count as perc_big_cells, b.n_st_msas
		from n_pctls a, all_cells_count b;
	quit;

	proc sql;
		create table &outdata. as
		select
			new_&variable._avg as avg label="Average Benes By MSA" format=COMMA8., 
			&variable._p1 as p1 label="p1", 
			&variable._p5 as p5 label="p5" format=COMMA8., 
			&variable._p10 as p10 label="p10" format=COMMA8., 
			&variable._p25 as p25 label="p25" format=COMMA8., 
			&variable._p50 as p50 label="p50" format=COMMA8., 
			&variable._p75 as p75 label="p75" format=COMMA8., 
			&variable._p90 as p90 label="p90" format=COMMA8., 
			&variable._p95 as p95 label="p95" format=COMMA8., 
			&variable._p99 as p99 label="p99" format=COMMA8., 
			new_&variable._min as min label="Min Benes By MSA" format=COMMA8., 
			new_&variable._max as max label="Max Benes By MSA" format=COMMA8., 
			n_st_msas label="St-MSA N" format=COMMA8.,
			sum_benes label="Total Benes" format=COMMA8.,
			big_cell_count/cell_count as pct_lg_msas label="Percent Large MSAs*" format=PERCENT8.2
		from big_cell_perc;
	quit;
%mend;

data masboe;
infile datalines dsd;
length col0 col1 col2 col3 col4 $24;
input col0 $ col1 $ col2 $ col3 $ col4 $;
datalines;
	05, Medicaid Only,Fee-For-Service,	Disability,	LTSS
	06, Medicaid Only,Fee-For-Service,	Disability,	No LTSS
	07, Medicaid Only,Fee-For-Service,	No Disability,	LTSS
	08, Medicaid Only,Fee-For-Service,	No Disability,	No LTSS
;

data nomasboe;
infile datalines dsd;
length col1 col2 col3 col4 $24;
input col1 $ col2 $ col3 $ col4 $;
datalines;
	'All children', 'All children', 'All children', 'All children' 
	'All adults', 'All adults', 'All adults', 'All adults' 
	'All seniors', 'All seniors', 'All seniors', 'All seniors' 
	'All ages','All ages','All ages','All ages'
;
run;

%macro put_tabs_together(n_dat, age_cat,agegroup);
	proc sql; 
		select col1 , col2, col3, col4, avg, p1, p5, p10, p25, p50, p75, p90, p95, p99, min, max, n_st_msas, sum_benes, pct_lg_msas
		from masboe a full join &n_dat. b
		on a.col0 = substr(b.new_age_servicetype,length(b.new_age_servicetype)-1,2)
		where agecat=&age_cat.
		union 
		select col1 , col2, col3, col4, avg, p1, p5, p10, p25, p50, p75, p90, p95, p99, min, max, n_st_msas, sum_benes, pct_lg_msas
		from (select * from nomasboe where col1 = &agegroup.), (select * from &n_dat._nomasboe where new_age_cat=&age_cat.)
		order by col1 desc;
	quit;
%mend;

%macro put_tabs_together_nomasboe(n_dat, age_cat,agegroup);
	proc sql; 
		select col1 , col2, col3, col4, avg, p1, p5, p10, p25, p50, p75, p90, p95, p99, min, max, n_st_msas, sum_benes, pct_lg_msas
		from masboe a full join &n_dat. b
		on a.col0 = substr(b.new_age_servicetype,length(b.new_age_servicetype)-1,2)
		where agecat=&age_cat.
		union 
		select col1 , col2, col3, col4, avg, p1, p5, p10, p25, p50, p75, p90, p95, p99, min, max, n_st_msas, sum_benes, pct_lg_msas
		from (select * from nomasboe where col1 = &agegroup.), (select * from &n_dat._nomasboe where new_age_cat=&age_cat.)
		order by col1 desc;
	quit;
%mend;

%macro put_tabs_together_noage(n_dat, n_dat_noagenomasboe, servicetype, agegroup);
	proc sql; 
		select col1 , col2, col3, col4, avg, p1, p5, p10, p25, p50, p75, p90, p95, p99, min, max, n_st_msas, sum_benes, pct_lg_msas
		from masboe a full join &n_dat. b
		on a.col0 = b.new_servicetype
		union 
		select col1 , col2, col3, col4, avg, p1, p5, p10, p25, p50, p75, p90, p95, p99, min, max, n_st_msas, sum_benes, pct_lg_msas
		from (select * from nomasboe where col1 = &agegroup.), (select * from &n_dat_noagenomasboe.)
		order by col1 desc;
	quit;
%mend;

%sum_tab(cdps_msa_dat=msa_dia2l,variable=cell_n,largecelln=100,outdata=dia2l_n_tab)
%sum_tab_nomasboe(cdps_msa_dat=msa_dia2l,variable=cell_n,largecelln=100,outdata=dia2l_n_tab_nomasboe)
%sum_tab_noage(cdps_msa_dat=msa_dia2l_noage,variable=cell_n,largecelln=100,outdata=dia2l_n_tab_noage)
%sum_tab_nomasboe_noage(cdps_msa_dat=msa_dia2l_nomasboe_noage,variable=cell_n,largecelln=100,outdata=dia2l_n_tab_nomasboe_noage)

%sum_tab(cdps_msa_dat=msa_carel,variable=cell_n,largecelln=100,outdata=carel_n_tab)
%sum_tab_nomasboe(cdps_msa_dat=msa_carel,variable=cell_n,largecelln=100,outdata=carel_n_tab_nomasboe)
%sum_tab_noage(cdps_msa_dat=msa_carel_noage,variable=cell_n,largecelln=100,outdata=carel_n_tab_noage)
%sum_tab_nomasboe_noage(cdps_msa_dat=msa_carel_nomasboe_noage,variable=cell_n,largecelln=100,outdata=carel_n_tab_nomasboe_noage)

%sum_tab(cdps_msa_dat=msa_canl,variable=cell_n,largecelln=100,outdata=canl_n_tab)
%sum_tab_nomasboe(cdps_msa_dat=msa_canl,variable=cell_n,largecelln=100,outdata=canl_n_tab_nomasboe)
%sum_tab_noage(cdps_msa_dat=msa_canl_noage,variable=cell_n,largecelln=100,outdata=canl_n_tab_noage)
%sum_tab_nomasboe_noage(cdps_msa_dat=msa_canl_nomasboe_noage,variable=cell_n,largecelln=100,outdata=canl_n_tab_nomasboe_noage)

%sum_tab(cdps_msa_dat=msa_prgcmp,variable=cell_n,largecelln=100,outdata=prgcmp_n_tab)
%sum_tab_nomasboe(cdps_msa_dat=msa_prgcmp,variable=cell_n,largecelln=100,outdata=prgcmp_n_tab_nomasboe)
%sum_tab_noage(cdps_msa_dat=msa_prgcmp_noage,variable=cell_n,largecelln=100,outdata=prgcmp_n_tab_noage)
%sum_tab_nomasboe_noage(cdps_msa_dat=msa_prgcmp_nomasboe_noage,variable=cell_n,largecelln=100,outdata=prgcmp_n_tab_nomasboe_noage)

%sum_tab(cdps_msa_dat=msa_psyl,variable=cell_n,largecelln=100,outdata=psyl_n_tab)
%sum_tab_nomasboe(cdps_msa_dat=msa_psyl,variable=cell_n,largecelln=100,outdata=psyl_n_tab_nomasboe)
%sum_tab_noage(cdps_msa_dat=msa_psyl_noage,variable=cell_n,largecelln=100,outdata=psyl_n_tab_noage)
%sum_tab_nomasboe_noage(cdps_msa_dat=msa_psyl_nomasboe_noage,variable=cell_n,largecelln=100,outdata=psyl_n_tab_nomasboe_noage)

%sum_tab(cdps_msa_dat=msa_pula,variable=cell_n,largecelln=100,outdata=pula_n_tab)
%sum_tab_nomasboe(cdps_msa_dat=msa_pula,variable=cell_n,largecelln=100,outdata=pula_n_tab_nomasboe)
%sum_tab_noage(cdps_msa_dat=msa_pula_noage,variable=cell_n,largecelln=100,outdata=pula_n_tab_noage)
%sum_tab_nomasboe_noage(cdps_msa_dat=msa_pula_nomasboe_noage,variable=cell_n,largecelln=100,outdata=pula_n_tab_nomasboe_noage)

%macro nomasboe_tab();
	ods excel file="&report_folder.\cdps_msa_distribution_&space_name..xlsx";
	ods excel options(sheet_name="Diabetes" sheet_interval="none" absolute_column_width='20,20,20,25,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10' title_footnote_nobreak="no");
		proc odstext; p "Child" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=dia2l_n_tab, age_cat=1,agegroup="All children");
		proc odstext; p "Adult" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=dia2l_n_tab, age_cat=2,agegroup="All adults");
		proc odstext; p "Senior" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=dia2l_n_tab, age_cat=3,agegroup="All seniors");
		proc odstext; p "All ages" / style=[color=black font_weight=bold]; run;
		%put_tabs_together_noage(n_dat=dia2l_n_tab_noage, n_dat_noagenomasboe=dia2l_n_tab_nomasboe_noage, agegroup="All ages")
		proc odstext; p "*Large MSAs have >= 100 benes"; run;
	&dum_tab.;
	ods excel options(sheet_name="Hypertension" sheet_interval="none");
		proc odstext; p "Child" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=carel_n_tab, age_cat=1,agegroup="All children");
		proc odstext; p "Adult" / style=[color=black font_weight=bold]; run;
			%put_tabs_together(n_dat=carel_n_tab, age_cat=2,agegroup="All adults");
		proc odstext; p "Senior" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=carel_n_tab, age_cat=3,agegroup="All seniors");
		proc odstext; p "All ages" / style=[color=black font_weight=bold]; run;
		%put_tabs_together_noage(n_dat=carel_n_tab_noage, n_dat_noagenomasboe=carel_n_tab_nomasboe_noage, agegroup="All ages")
		proc odstext; p "*Large MSAs have >= 100 benes"; run;
	&dum_tab.;
	ods excel options(sheet_name="Colon, prostate, cervical cancers" sheet_interval="none");
		proc odstext; p "Child" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=canl_n_tab, age_cat=1,agegroup="All children");
		proc odstext; p "Adult" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=canl_n_tab, age_cat=2,agegroup="All adults");
		proc odstext; p "Senior" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=canl_n_tab, age_cat=3,agegroup="All seniors");
		proc odstext; p "All ages" / style=[color=black font_weight=bold]; run;
		%put_tabs_together_noage(n_dat=canl_n_tab_noage, n_dat_noagenomasboe=canl_n_tab_nomasboe_noage, agegroup="All ages")
		proc odstext; p "*Large MSAs have >= 100 benes"; run;
	&dum_tab.;
	ods excel options(sheet_name="Completed Pregnancy" sheet_interval="none");
		proc odstext; p "Child" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=prgcmp_n_tab, age_cat=1,agegroup="All children");
		proc odstext; p "Adult" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=prgcmp_n_tab, age_cat=2,agegroup="All adults");
		proc odstext; p "Senior" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=prgcmp_n_tab, age_cat=3,agegroup="All seniors");
		proc odstext; p "All ages" / style=[color=black font_weight=bold]; run;
		%put_tabs_together_noage(n_dat=prgcmp_n_tab_noage, n_dat_noagenomasboe=prgcmp_n_tab_nomasboe_noage, agegroup="All ages")
		proc odstext; p "*Large MSAs have >= 100 benes"; run;
	&dum_tab.;
	ods excel options(sheet_name="Anxiety, Depression, Phobia" sheet_interval="none");
		proc odstext; p "Child" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=psyl_n_tab, age_cat=1,agegroup="All children");
		proc odstext; p "Adult" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=psyl_n_tab, age_cat=2,agegroup="All adults");
		proc odstext; p "Senior" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=psyl_n_tab, age_cat=3,agegroup="All seniors");
		proc odstext; p "All ages" / style=[color=black font_weight=bold]; run;
		%put_tabs_together_noage(n_dat=psyl_n_tab_noage, n_dat_noagenomasboe=psyl_n_tab_nomasboe_noage, agegroup="All ages")
		proc odstext; p "*Large MSAs have >= 100 benes"; run;
	&dum_tab.;
	ods excel options(sheet_name="Asthma" sheet_interval="none");
		proc odstext; p "Child" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=pula_n_tab, age_cat=1,agegroup="All children");
		proc odstext; p "Adult" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=pula_n_tab, age_cat=2,agegroup="All adults");
		proc odstext; p "Senior" / style=[color=black font_weight=bold]; run;
		%put_tabs_together(n_dat=pula_n_tab, age_cat=3,agegroup="All seniors");
		proc odstext; p "All ages" / style=[color=black font_weight=bold]; run;
		%put_tabs_together_noage(n_dat=pula_n_tab_noage, n_dat_noagenomasboe=pula_n_tab_nomasboe_noage, agegroup="All ages")
		proc odstext; p "*Large MSAs have >= 100 benes"; run;
	ods excel close;
%mend;

%nomasboe_tab()
