/*******************************************************************************************************************/ 
/*	Purpose: Collage CDPS-related claims subset to the MSA level	
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak
/*	Notes: 
/*		1) Collapse macros for easier manipulation
/*******************************************************************************************************************/ 
/*Options to change*/
%macro prod();
	options obs=MAX;
	/*Log*/
	proc printto print="P:\MCD-SPVR\log\06_cdps_percentiles_&sysdate..lst"
	               log="P:\MCD-SPVR\log\06_cdps_percentiles_&sysdate..log" NEW;
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
libname data_ot    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS\OT";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;
libname cpds_wt "P:\MCD-SPVR\data\NO_PII\CDPS_WEIGHTS";
libname scores  "P:\MCD-SPVR\data\workspace\CDPS_SCORES";
libname ahrf_hrr "\\sas1_alt\MCD-SPVR\data\NO_PII\HRR\workspace";

%let in_data = space.max2012_lt_ot_cdps; /*created in 13_cdps_reports.sas*/
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
%macro msa;
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
%msa;

/***************************/
/*Collapse to specified var*/
/***************************/
%macro collapse (indata,collapse_on,outdata,cdpsflag);
	proc sql;
		create table &outdata. as
			select year, age_servicetype, age_cat, &collapse_on., dis_cat, dual_cat, foster_cat, ltss_cat, mc_cat,
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
			mean(NOCDPS) as no_cdps_conds,
			mean(pspend_i) as pred_mcd_spd
			from &indata. 
			where &cdpsflag=1
			group by year, age_servicetype, age_cat, &collapse_on.,dis_cat, dual_cat, foster_cat, ltss_cat, mc_cat;
		quit;

%mend;
%collapse(indata=max_msa_2012,collapse_on=st_msa,outdata=msa_dia2l, cdpsflag=dia2l);

proc sql;
	create table age_servicetypes as 
	select distinct(age_servicetype)
	from msa_dia2l;

	create table unique_msas as
	select distinct st_msa
	from ahrf_msa_xwalk;

	create table msas as
	select *, 0 as cell_n,
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
		coalesce(a.cell_n,b.cell_n) as new_cell_n,
		coalesce(a.age_cat,b.age_cat) as new_age_cat
	from msa_dia2l a full join msas b
	on a.st_msa = b.st_msa and a.age_servicetype=b.age_servicetype;
quit;

	proc univariate data=ageservicetypemsas noprint;
		class new_age_servicetype;
		var new_cell_n;
		output out=n_pctls
		pctlpts = 1 5 10 25 50 75 90 95 99
		pctlpre=cell_n_p
		mean=new_cell_n_avg
		min=new_cell_n_min
		max=new_cell_n_max
		sum=sum_benes;
	run;

	proc sql;
		create table all_cells_count as
		select * from
			(select new_age_servicetype, count(*) as cell_count
			from ageservicetypemsas
			group by new_age_servicetype) a 
		inner join  
			(select new_age_servicetype, count(new_cell_n) as big_cell_count
			from ageservicetypemsas
			where new_cell_n>=100
			group by new_age_servicetype) b
		on a.new_age_servicetype = b.new_age_servicetype
		inner join
			(select new_age_servicetype, count(distinct st_msa) as n_st_msas
			from ageservicetypemsas
			group by new_age_servicetype) c
		on a.new_age_servicetype = c.new_age_servicetype;

		create table big_cell_perc as
		select a.*, b.cell_count, b.big_cell_count, b.big_cell_count/b.cell_count as perc_big_cells, b.n_st_msas
		from n_pctls a left join all_cells_count b 
		on a.new_age_servicetype=b.new_age_servicetype;
	quit;

%let variable = cell_n;
	proc sql;
		select new_age_servicetype,
			new_&variable._avg label="Average Benes By MSA" format=COMMA8., 
			&variable._p1 label="p1", &variable._p5 label="p5" format=COMMA8., 
			&variable._p10 label="p10", &variable._p25 label="p25" format=COMMA8., 
			&variable._p50 label="p50", &variable._p75 label="p75" format=COMMA8., 
			&variable._p90 label="p90", &variable._p95 label="p95" format=COMMA8., 
			&variable._p99 label="p99" format=COMMA8., 
			new_&variable._min label="Min Benes By MSA" format=COMMA8., 
			new_&variable._max label="Max Benes By MSA" format=COMMA8., 
			cell_count label="St-MSA N" format=COMMA8.,
			sum_benes label="Total Benes" format=COMMA8.,
			big_cell_count/cell_count label="Percent Large MSAs" format=PERCENT8.2
		from big_cell_perc;
	quit;


