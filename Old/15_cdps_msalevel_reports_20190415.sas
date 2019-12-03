proc sql;
    create table dia2l_allclaims as
    select *,
        case when service_type = "IP_HOSP" then inpt_spd
        when service_type = "NURS_FAC" then lt_spd
        when service_type = "PHYS_CLIN" then physclin_spd
        when service_type = "HOSP_OPT" then ot_spd
        when service_type = "RX_DRUG" then rx_spd
        when service_type = "TOTAL" then TOT_MDCD_PYMT_AMT
        else .
        end as mdcd_pymt_amt,
        case when service_type = "IP_HOSP" then inpt_clm
        when service_type = "NURS_FAC" then lt_clm
        when service_type = "PHYS_CLIN" then physclin_clm
        when service_type = "HOSP_OPT" then ot_clm
        when service_type = "RX_DRUG" then rx_clm
        when service_type = "TOTAL" then cell_n
        end as clm_count
    from max_cdps, unique_servicetype_wother
    where DIA2L=1;
quit;
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
	/*proc printto print="P:\MCD-SPVR\log\15_cdps_msalevel_reports&sysdate..lst"
	               log="P:\MCD-SPVR\log\15_cdps_msalevel_reports&sysdate..log" NEW;
	run;*/
%mend prod;

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
/*cell_n is the claims count, NOT bene count
/*remove seniors per feedback from MACPAC*/
/*
%macro cdps;
	*1 concatenate state cdps files (DATA);
	data work.cdps_allst;
		set scores.cdps_asth:;
		state_cd=substr(recipno,1,2);
	run;

	*2 join CDPS to categories_full by RECIPNO (SQL);
	proc sql;
		create table space.max_cdpsscores  as
		select *
		from space.id_pop_25feb2019 a left join cdps_allst (drop= male age state_cd) b 
		on a.recipno = b.RECIPNO;
	quit;
%mend;
%cdps;
*/
proc sql;
	create table no_seniors_ip_lt as
	select *
	from SPACE.IP_LT_CDPSCLAIMS_15APR2019 a left join space.max_cdpsscores b/*input data file from 05_subset_claims_byDX*/
	on a.bene_id = b.bene_id
	where b.age_cat ne 3 and a.bene_id ne '';

	create table no_seniors_ot_canytx as
	select *
	from space.ot_bene_svc_canytx  a left join space.max_cdpsscores b/*input data file from 05_subset_claims_byDX*/
	on a.bene_id = b.bene_id
	where b.age_cat ne 3 and a.bene_id ne '';

	create table no_seniors_ot1_25 as
	select *
	from SPACE.OT_BENE_SVC_1_25  a left join space.max_cdps_2012 b/*input data file from 05_subset_claims_byDX*/
	on a.bene_id = b.bene_id
	where b.age_cat ne 3 and a.bene_id ne '';

	create table no_seniors_ot26_56 as
	select *
	from SPACE.OT_BENE_SVC_26_56  a left join space.max_cdpsscores b/*input data file from 05_subset_claims_byDX*/
	on a.bene_id = b.bene_id
	where b.age_cat ne 3 and a.bene_id ne '';
quit; 

data no_snrs;
	length service_type $8;
	set no_seniors:;
run;

%let indata_max = no_snrs; 
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
/********************************************************/
/*Initial processing to attach MSA info to files*/
/********************************************************/

/*limit claims to only those with CDPS-related diagnosis*/
proc sql;
	/*only claims with diagnosis related to CDPS flag*/
	create table dia2l_dxclaims as
	select *
	from &indata_max.
	where svc_dx = "DIAB" and DIA2L = 1 ;

	create table dia2l_allclaims as
	select *
	from &indata_max.
	where DIA2L = 1;
quit;
proc sql;
	create table carel_dxclaims as
	select *
	from &indata_max.
	where svc_dx = "HYPT" and CAREL=1;

	create table carel_allclaims as
	select *
	from &indata_max.
	where CAREL = 1;
quit;
proc sql;
	create table canl_dxclaims as
	select *
	from &indata_max.
	where svc_dx = "CANC" and CANL =1;
	
	create table canl_allclaims as
	select *
	from &indata_max.
	where CANL=1;
proc sql;
	create table prgcmp_dxclaims as
	select *
	from &indata_max.
	where svc_dx = "PREG" and PRGCMP=1;

	create table prgcmp_allclaims as
	select *
	from &indata_max.
	where PRGCMP=1;
quit;
proc sql;
	create table psyl_dxclaims as
	select *
	from &indata_max.
	where svc_dx = "PSYC" and PSYL =1;
	
	create table psyl_allclaims as
	select *
	from &indata_max.
	where PSYL=1;
quit;
proc sql;
	create table pula_dxclaims as
	select *
	from &indata_max.
	where svc_dx = "ASTH" and PULA=1;
	
	create table pula_allclaims as
	select *
	from &indata_max.
	where PULA=1;
quit;

*add ahrf/msa data to claims;
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
/*so every msa/service type is represented*/
	data unique_servicetype;
		input service_type $;
		datalines;
		EMERG_ROOM
		PHYS_CLIN
		HOSP_OPT
		RX_DRUG
		NURS_FAC
		IP_HOSP
		;
	run;

%let in_data=pula_dxclaims_ffs_nodual; %let collapse_on=st_msa; %let in_uniqueservicetypes=unique_servicetype; %let orig_data=ahrf_aggre; %let out_data=pula_dxclaims;

/***************************/
/*Collapse to specified var*/
/***************************/
*%macro collapse(in_data,collapse_on,in_uniqueservicetypes,orig_data,out_data);
	*get means of spending and CDPS and generate mult_c=(spend_c/cdps_c);
	*we are not looking at age or service categories any more so this is a mean by CDPS category;
	proc sql;
		create table spendavg as
		select service_type,
			AVG(TOT_MDCD_PYMT_AMT) as mspend_c,
			AVG(CDPS_SCORE) as cdps_c,
			AVG(TOT_MDCD_PYMT_AMT) / AVG(CDPS_SCORE) as mult_c
		from &in_data.
		group by service_type;
	quit;

	*join means to individual records by 'chip','nmcd','','msg'  and 
	   generate pspend_i=spend_i*mult_c and rspend_i=spend_i-pspend_i (SQL);
	proc sql;
		create table &out_data._res as
		select T1.*,
			(T1.TOT_MDCD_PYMT_AMT) AS mspend_i,
			T2.mspend_c,
			T2.cdps_c,
			T2.mult_c,
			T1.CDPS_SCORE*T2.mult_c AS Pspend_i,
			(T1.TOT_MDCD_PYMT_AMT) - T1.CDPS_SCORE*T2.mult_c AS Rspend_i
		from &in_data. /*(drop= mspend_i mspend_c cdps_c mult_c Pspend_i Rspend_i)*/ T1 left join spendavg T2 /*drop those vars bc they were calculated for the entire population, not CDPS-specific*/
		on T1.service_type=T2.service_type;
	quit;
	proc sql ;
		create table max_2012_msa_join as
		select  a.*, b.*
		from &out_data._res a left join ahrf_msa_xwalk (drop=year) b
		on a.county=b.st_cnty;
	quit;
	/*check which msas don't have matches*/
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
		create table &out_data._msas as
		select *
		from max_2012_msa_join
		where st_msa ne ' ';
	quit;
	/*get top coded spending vars*/
	proc univariate data=&out_data._msas noprint;
		class &collapse_on. service_type;
		var mdcd_pymt_amt Rspend_i;
		output out=spend_cap_&collapse_on.
		pctlpts = 99.5
		pctlpre=mdcd_spd_p res_spd_p;
	run;

	proc sql;
	  create table &out_data._msas_spdTC AS
		select a.*, 
			case when a.mdcd_pymt_amt>B.mdcd_spd_p99_5 then B.mdcd_spd_p99_5 else a.mdcd_pymt_amt end as mdcd_spdTC,
			case when a.Rspend_i>B.res_spd_p99_5 then B.res_spd_p99_5 else a.Rspend_i end as res_spdTC
		from &out_data._msas a left join spend_cap_&collapse_on. b  
		on a.&collapse_on.=b.&collapse_on.;
	  quit;
	proc sql;
		create table &out_data._collapsebenes as
			select &collapse_on., service_type,
			/*demographic vars*/
			sum(ltss_cat) as sum_ltss,
			sum(dis_cat) as sum_dis,
			count(distinct bene_id) as bene_count,
			/*spending vars per bene*/
			sum(mdcd_pymt_amt) as mdcd_spd_sum,
			sum(mdcd_spdTC) as mdcd_spdTC_sum,
			sum(pspend_i) as pred_mdcd_spd_sum,
			sum(Rspend_i) as res_spd_sum,
			sum(res_spdTC) as res_spdTC_sum,

			/*utilization vars */
			sum(CLM_CNT) as clm_sum,
			from &out_data._msas_spdTC 
			group by &collapse_on., service_type;
		quit;

	proc sort data=&out_data._collapsebenes;
		by st_msa;
	run;
	proc transpose data=&out_data._collapsebenes out=mdcd (drop=_NAME_) prefix=mdcd_pymt_;
	    by st_msa ;
	    id service_type ;
	    var mdcd_spd_sum ;
	run;	
	proc transpose data=&out_data._collapsebenes out=clm (drop=_NAME_) prefix=clm_sum_;
	    by st_msa ;
	    id service_type ;
	    var clm_sum ;
	run;	
	proc transpose data=&out_data._collapsebenes out=ltss (drop=_NAME_) prefix=ltss_sum_;
	    by st_msa ;
	    id service_type ;
	    var sum_ltss ;
	run;
	proc transpose data=&out_data._collapsebenes out=dis (drop=_NAME_) prefix=dis_sum_;
	    by st_msa ;
	    id service_type ;
	    var sum_dis ;
	run;
	proc transpose data=&out_data._collapsebenes out=bene (drop=_NAME_) prefix=bene_count_;
	    by st_msa ;
	    id service_type ;
	    var bene_count ;
	run;
	/* other vars to join: mdcd_spdTC_sum pred_mdcd_spd_sum res_spd_sum res_spdTC_sum*/  
	proc sql;
		create table &out_data._msalevel (drop = bene_count_HOSP_OPT bene_count_IP_HOSP bene_count_PHYS_CLI bene_count_NURS_FAC bene_count_EMERG_RO) as
		select *, sum(bene_count_HOSP_OPT,bene_count_IP_HOSP,bene_count_PHYS_CLI, bene_count_NURS_FAC,bene_count_EMERG_RO) as sum_benes
		from mdcd a full join clm b on a.st_msa = b.st_msa
			full join ltss c on a.st_msa = c.st_msa
			full join dis d on a.st_msa = d.st_msa
			full join bene e on a.st_msa = e.st_msa;
	quit;
	proc print data=&out_data._msalevel (obs=10);run;
	proc sql;
		select name into :mdcd separated by " "
		from dictionary.columns
		where libname = upcase("work") and memname = upcase("&out_data._msalevel") and name like "mdcd_pymt%";
	quit;

	/*get spending percentiles*/
	proc univariate data=&out_data._msas_spdTC noprint;
		class &collapse_on. service_type;
		var mdcd_pymt_amt Rspend_i;
		output out=spend_pctls_&collapse_on.
		pctlpts = 10 25 50 75 90 95 99
		pctlpre=mdcd_spd_p res_spd_p;
	run;

	/*get basic summary stats for each MSA*/
	proc sql;
		create table &out_data._collapsed as
			select year, &collapse_on., service_type,
			/*demographic vars*/
			count(distinct bene_id) as bene_count,
			sum(cell_n) as cell_n,
			sum(mo_dual) as dual_mon, 
			sum(mo_mc) as mc_mon, 
			sum(mo_dsbl) as dis_mon, 
			sum(mo_ltss) as ltss_mon,
			sum(EL_ELGBLTY_MO_CNT) as elg_mon, 
			sum(d_servicetype_n) as d_servicetype_n, 
			sum(died_n) as died_n,
			sum(mas_cash) as mas_cash_n, 
			sum(mas_cash)/(count(distinct bene_id)) as mas_cash,
			sum(mas_mn) as mas_mn_n, 
			sum(mas_mn)/(count(distinct bene_id)) as mas_mn, 
			sum(mas_pov) as mas_pov_n, 
			sum(mas_pov)/(count(distinct bene_id)) as mas_pov, 
			sum(mas_1115) as mas_1115_n,
			sum(mas_1115)/(count(distinct bene_id)) as mas_1115,
			sum(mas_oth) as mas_oth_n, 
			sum(mas_oth)/(count(distinct bene_id)) as mas_oth, 
			sum(boe_aged) as boe_aged_n, 
			sum(boe_aged)/(count(distinct bene_id)) as boe_aged, 
			sum(boe_disabled) as boe_disabled_n, 
			sum(boe_disabled)/(count(distinct bene_id)) as boe_disabled, 
			sum(boe_child) as boe_child_n, 
			sum(boe_child)/(count(distinct bene_id)) as boe_child, 
			sum(boe_adult) as boe_adult_n, 
			sum(boe_adult)/(count(distinct bene_id)) as boe_adult, 
			sum(boe_uchild) as boe_uchild_n, 
			sum(boe_uchild)/(count(distinct bene_id)) as boe_uchild, 
			sum(boe_uadult) as boe_uadult_n, 
			sum(boe_uadult)/(count(distinct bene_id)) as boe_uadult, 
			sum(boe_fchild) as boe_fchild_n,
			sum(boe_fchild)/(count(distinct bene_id)) as boe_fchild,
			sum(male) as male_n, 
			sum(male)/(count(distinct bene_id)) as male, 
			sum(age_0) as _0_n, 
			sum(age_0)/(count(distinct bene_id)) as _0, 
			sum(age_1_5) as _1_5_n, 
			sum(age_1_5)/(count(distinct bene_id)) as _1_5, 
			sum(age_6_18) as _6_18_n, 
			sum(age_6_18)/(count(distinct bene_id)) as _6_18, 
			sum(age_19_44) as _19_44_n, 
			sum(age_19_44)/(count(distinct bene_id)) as _19_44, 
			sum(age_45_64) as _45_64_n, 
			sum(age_45_64)/(count(distinct bene_id)) as _45_64, 
			sum(age_65_84) as _65_84_n, 
			sum(age_65_84)/(count(distinct bene_id)) as _65_84, 
			sum(age_85p) as _85p_n, 
			sum(age_85p)/(count(distinct bene_id)) as _85p,
			mean(CDPS_SCORE) as cdps,
			mean(NOCDPS) as no_cdps_conds,

			/*spending vars per bene*/
			sum(mdcd_pymt_amt)/ count(distinct bene_id) as mdcd_spd_sum,
			min(mdcd_pymt_amt)/ count(distinct bene_id) as mdcd_spd_min,
			mean(mdcd_pymt_amt)/ count(distinct bene_id) as mdcd_spd_mean,
			max(mdcd_pymt_amt)/ count(distinct bene_id) as mdcd_spd_max,
			stderr(mdcd_pymt_amt)/ count(distinct bene_id) as mdcd_spd_stderr,

			sum(mdcd_spdTC)/ count(distinct bene_id) as mdcd_spdTC_sum,
			min(mdcd_spdTC)/ count(distinct bene_id) as mdcd_spdTC_min,
			mean(mdcd_spdTC)/ count(distinct bene_id) as mdcd_spdTC_mean,
			max(mdcd_spdTC)/ count(distinct bene_id) as mdcd_spdTC_max,
			stderr(mdcd_spdTC)/ count(distinct bene_id) as mdcd_spdTC_stderr,

			sum(pspend_i)/ count(distinct bene_id) as pred_mdcd_spd_sum,
			min(pspend_i)/ count(distinct bene_id) as pred_mdcd_spd_min,
			mean(pspend_i)/ count(distinct bene_id) as pred_mdcd_spd_mean,
			max(pspend_i)/ count(distinct bene_id) as pred_mdcd_spd_max,
			stderr(pspend_i)/ count(distinct bene_id) as pred_mdcd_spd_stderr,

			sum(Rspend_i)/ count(distinct bene_id) as res_spd_sum,
			min(Rspend_i)/ count(distinct bene_id) as res_spd_min,
			mean(Rspend_i)/ count(distinct bene_id) as res_spd_mean,
			max(Rspend_i)/ count(distinct bene_id) as res_spd_max,
			stderr(Rspend_i)/ count(distinct bene_id) as res_spd_stderr,

			sum(res_spdTC)/ count(distinct bene_id) as res_spdTC_sum,
			min(res_spdTC)/ count(distinct bene_id) as res_spdTC_min,
			mean(res_spdTC)/ count(distinct bene_id) as res_spdTC_mean,
			max(res_spdTC)/ count(distinct bene_id) as res_spdTC_max,
			stderr(res_spdTC)/ count(distinct bene_id) as res_spdTC_stderr,

			/*utilization vars per bene*/
			sum(CLM_CNT)/ count(distinct bene_id) as clm_sum,
			min(CLM_CNT)/ count(distinct bene_id) as clm_min,
			mean(CLM_CNT)/ count(distinct bene_id) as clm_mean,
			max(CLM_CNT)/ count(distinct bene_id) as clm_max,
			stderr(CLM_CNT)/ count(distinct bene_id) as clm_stderr

			from &out_data._msas_spdTC
			group by year, &collapse_on., service_type;
		quit;

	proc sql;
		/*join the beds, md, urate, and povrate to msa data*/
		create table arhf_vars_added as 
		select a.*, b.beds, b.md, b.urate, b.povrate
		from &out_data._collapsed as a left join 
			(select distinct &collapse_on., beds, md, urate, povrate from &orig_data.) as b
		on a.&collapse_on.=b.&collapse_on.;
	quit;

	proc univariate data=&out_data._msas noprint;
		class &collapse_on. service_type;
		var CLM_CNT ;
		output out=count_pctls_&collapse_on.
		pctlpts = 10 25 50 75 90 95 99
		pctlpre=clm_p;
	run;

	/*put overall stats into final table*/
	proc sql;
		create table unique_msas as
		select distinct st_msa
		from ahrf_msa_xwalk;

		create table msas_servictypes as
		select *
		from &in_uniqueservicetypes., unique_msas;

		create table fintab_&collapse_on._ac (drop=st_msa service_type rename=(new_st_msa=st_msa new_service_type=service_type)) as
		select a.*,  
		b.*,
		c.*,
		coalesce(a.&collapse_on.,d.&collapse_on.) as new_st_msa, coalesce(a.service_type,d.service_type) as new_service_type
		from arhf_vars_added a 
			full join spend_pctls_&collapse_on. b on a.&collapse_on.=b.&collapse_on. and a.service_type = b.service_type
			full join count_pctls_&collapse_on. c on a.&collapse_on.=c.&collapse_on. and a.service_type = c.service_type
			full join msas_servictypes d on a.&collapse_on.=d.&collapse_on. and a.service_type = d.service_type
		where d.&collapse_on. ne '' and d.service_type ne '';
	quit;

	%let maxvars = dual_mon mc_mon dis_mon ltss_mon elg_mon cell_n d_servicetype_n died_n mas_: boe_: male: _0: _1_5: _6_18: _19_44: _45_64: _65_84: _85p: cdps spd: res:;

	*mark too-small cells missing;
	
	data space.&out_data._collapsed ;
		length st_msa $8.;
		set fintab_&collapse_on._ac;
		label 	
			st_msa ="State-MSA Code"
			dual_mon ="Number of Person Months of Dual Eligibility"
			mc_mon ="Number of Person Months of Managed Care Enrollment"
			dis_mon ="Number of Person Months of Disability"
			ltss_mon ="Number of Person Months of LTSS Use"
			elg_mon ="Number of Person Months of Eligibility"
			cell_n ="Number of Claims"
			bene_count = "Number of Beneficiaries"
			d_servicetype_n ="Number of Unique Statuses"
			died_n ="Number Dying in Year"
			mas_cash_n ="MAS Cash Beneficiaries (N)"
			mas_cash ="MAS Cash Beneficiaries (%)"
			mas_mn_n ="MAS Medically Needy Beneficiaries (N)"
			mas_mn ="MAS Medically Needy Beneficiaries (%)"
			mas_pov_n ="MAS Poverty-Related Beneficiaries (N)"
			mas_pov ="MAS Poverty-Related Beneficiaries (%)"
			mas_oth_n ="MAS Other Beneficiaries (N)"
			mas_oth ="MAS Other Beneficiaries (%)"
			mas_1115_n ="MAS 1115 Exspansion Beneficiaries (N)"
			mas_1115 ="MAS 1115 Exspansion Beneficiaries (%)"
			boe_aged_n ="BOE Aged Beneficiaries (N)"
			boe_aged ="BOE Aged Beneficiaries (%)"
			boe_disabled_n ="BOE Disabled Beneficiaries (N)"
			boe_disabled ="BOE Disabled Beneficiaries (%)"
			boe_child_n ="BOE Child Beneficiaries (N)"
			boe_child ="BOE Child Beneficiaries (%)"
			boe_adult_n ="BOE Adult Beneficiaries (N)"
			boe_adult ="BOE Adult Beneficiaries (%)"
			boe_uchild_n ="BOE Child (Unemployed Adult) Beneficiaries (N)"
			boe_uchild ="BOE Child (Unemployed Adult) Beneficiaries (%)"
			boe_uadult_n ="BOE Unemployed Adult Beneficiaries (N)"
			boe_uadult ="BOE Unemployed Adult Beneficiaries (%)"
			boe_fchild_n ="BOE Foster Child Beneficiaries (N)"
			boe_fchild ="BOE Foster Child Beneficiaries (%)"
			male_n ="Number of Male Beneficiaries (N)"
			male ="Number of Male Beneficiaries (%)"
			_0_n ="Number of Beneficiaries Age less than 1 year (N)"
			_0 ="Number of Beneficiaries Age less than 1 year (%)"
			_1_5_n ="Number of Beneficiaries Age 1 to 5 (N)"
			_1_5 ="Number of Beneficiaries Age 1 to 5 (%)"
			_6_18_n ="Number of Beneficiaries Age 6 to 18 (N)"
			_6_18 ="Number of Beneficiaries Age 6 to 18 (%)"
			_19_44_n ="Number of Beneficiaries Age 19 to 44 (N)"
			_19_44 ="Number of Beneficiaries Age 19 to 44 (%)"
			_45_64_n ="Number of Beneficiaries Age 45 to 64 (N)"
			_45_64 ="Number of Beneficiaries Age 45 to 64 (%)"
			_65_84_n ="Number of Beneficiaries Age 65 to 84 (N)"
			_65_84 ="Number of Beneficiaries Age 65 to 84 (%)"
			_85p_n ="Number of Beneficiaries Age 85 and above (N)"
			_85p ="Number of Beneficiaries Age 85 and above (%)"
			cdps="Mean CDPS Score"
			no_cdps_conds="Proportion of beneficiaries with no CDPS diagnoses in year"
			beds ="Number of hospital beds per 1k people, 2010"
			md ="Number of physicians per 1k people, 2010"
			urate ="Unemployment rate, 2012"
			povrate ="Rate of persons in poverty, 2012"
			service_type = "Service type"

			pred_mdcd_spd_sum ="Sum Annual Predicted Spending Per Beneficiary"
			pred_mdcd_spd_min ="Minimum Annual Predicted Spending Per Beneficia"
			pred_mdcd_spd_mean ="Mean Annual Predicted Spending Per Beneficia"
			pred_mdcd_spd_max ="Maximum Annual Predicted Spending Per Beneficia"
			pred_mdcd_spd_stderr ="Standard Error of Mean Annual Predicted Spending"

			mdcd_spdTC_min ="Minimum Annual Spending Per Beneficiary (top coded)"
			mdcd_spdTC_mean ="Mean Annual Spending Per Beneficiary (top coded)"
			mdcd_spdTC_max ="Maximum Annual Spending Per Beneficiary (top coded)"
			mdcd_spdTC_stderr ="Standard Error of Mean Annual Spending Per Beneficiary (top coded)"
			mdcd_spdTC_sum ="Sum Annual Spending Per Beneficiary (top coded)"
			mdcd_spd_sum ="Sum Annual Spending Per Beneficiary"
			mdcd_spd_min ="Minimum Annual Spending Per Beneficia"
			mdcd_spd_mean ="Mean Annual Spending Per Beneficia"
			mdcd_spd_max ="Maximum Annual Spending Per Beneficia"
			mdcd_spd_stderr ="Standard Error of Mean Annual Spending"
			mdcd_spd_p10 ="10th Percentile of Annual Spending"
			mdcd_spd_p25 ="25th Percentile of Annual Spending"
			mdcd_spd_p50 ="50th Percentile of Annual Spending"
			mdcd_spd_p75 ="75th Percentile of Annual Spending"
			mdcd_spd_p90 ="90th Percentile of Annual Spending"
			mdcd_spd_p95 ="95th Percentile of Annual Spending"
			mdcd_spd_p99 ="99th Percentile of Annual Spending"

			res_spdTC_min ="Minimum Residual Annual Spending Per Beneficiary (top coded)"
			res_spdTC_mean ="Mean Residual Annual Spending Per Beneficiary (top coded)"
			res_spdTC_sum ="Sum Residual Annual Spending Per Beneficiary (top coded)"
			res_spdTC_max ="Maximum Residual Annual Spending Per Beneficiary (top coded)"
			res_spdTC_stderr ="Standard Error of Mean Residual Annual Spending Per Beneficiary (top coded)"
			res_spd_sum ="Sum Residual Annual Spending Per Beneficiary"
			res_spd_min ="Minimum Residual Annual Spending Per Beneficiary"
			res_spd_mean ="Mean Residual Annual Spending Per Beneficiary"
			res_spd_max ="Maximum Residual Annual Spending Per Beneficiary"
			res_spd_stderr ="Standard Error of Mean Residual Annual Spending"
			res_spd_p10 ="10th Percentile of Residual Annual Spending"
			res_spd_p25 ="25th Percentile of Residual Annual Spending"
			res_spd_p50 ="50th Percentile of Residual Annual Spending"
			res_spd_p75 ="75th Percentile of Residual Annual Spending"
			res_spd_p90 ="90th Percentile of Residual Annual Spending"
			res_spd_p95 ="95th Percentile of Residual Annual Spending"
			res_spd_p99 ="99th Percentile of Residual Annual Spending"

			clm_sum ="Sum Claims Per Beneficiary"
			clm_min ="Minimum Claims Per Beneficiary"
			clm_mean ="Mean Claims Per Beneficiary"
			clm_max ="Maximum Claims Per Beneficiary"
			clm_stderr ="Standard Error of Mean Claims"
			clm_p10 ="10th Percentile of Claims"
			clm_p25 ="25th Percentile of Claims"
			clm_p50 ="50th Percentile of Claims"
			clm_p75 ="75th Percentile of Claims"
			clm_p90 ="90th Percentile of Claims"
			clm_p95 ="95th Percentile of Claims"
			clm_p99 ="99th Percentile of Claims" 
			;
	
			array all_cells {*} &maxvars.; 
			if bene_count<11 then do;
				do i=1 to dim(all_cells);
					all_cells(i)=.;
				end;
			end;
			drop i;
	
	run;
%mend collapse;

%macro ffs_nodual(indata);
	proc sql;
		create table &indata._ffs_nodual as
		select *
		from &indata.
		where substr(age_servicetype,length(age_servicetype)-1,2) in ("05","06","07","08");
	quit;
%mend;
%ffs_nodual(indata=dia2l_dxclaims);
%ffs_nodual(indata=carel_dxclaims);
%ffs_nodual(indata=canl_dxclaims);
%ffs_nodual(indata=prgcmp_dxclaims);
%ffs_nodual(indata=psyl_dxclaims);
%ffs_nodual(indata=pula_dxclaims);

%collapse(in_data=dia2l_dxclaims_ffs_nodual,collapse_on=st_msa, orig_data=ahrf_aggre,out_data=dia2l_dxclaims);
%collapse(in_data=carel_dxclaims_ffs_nodual,collapse_on=st_msa, orig_data=ahrf_aggre,out_data=carel_dxclaims);
%collapse(in_data=canl_dxclaims_ffs_nodual,collapse_on=st_msa, orig_data=ahrf_aggre,out_data=canl_dxclaims);
%collapse(in_data=prgcmp_dxclaims_ffs_nodual,collapse_on=st_msa, orig_data=ahrf_aggre,out_data=prgcmp_dxclaims);
%collapse(in_data=psyl_dxclaims_ffs_nodual,collapse_on=st_msa, orig_data=ahrf_aggre,out_data=psyl_dxclaims);
%collapse(in_data=pula_dxclaims_ffs_nodual,collapse_on=st_msa, orig_data=ahrf_aggre,out_data=pula_dxclaims);

/*All claims*/
proc sql;
	create table max_cdps as
	select a. *, b.TOT_IP_STAY_CNT AS inpt_clm, b.TOT_IP_DAY_CNT_STAYS, b.FFS_PYMT_AMT_01 as inpt_spd, (b.FFS_CLM_CNT_08 + b.FFS_CLM_CNT_12) as physclin_clm, 
		(b.FFS_PYMT_AMT_08 + b.FFS_PYMT_AMT_12) as physclin_spd, b.FFS_CLM_CNT_16 as rx_clm, b.FFS_PYMT_AMT_16 as rx_spd, FFS_CLM_CNT_07 as lt_clm, FFS_PYMT_AMT_07 as lt_spd,
		FFS_CLM_CNT_11 as ot_clm, FFS_PYMT_AMT_11 as ot_spd,
		substr(age_servicetype, length(age_servicetype)-1,2) as masboe_cat
	from space.max_cdpsscores a left join data.maxdata_ps_2012 b
	on a.bene_id = b.bene_id
	where (dia2l = 1 or carel = 1 or canl = 1 or prgcmp = 1 or psyl = 1 or pula = 1) and calculated masboe_cat in ("05","06","07","08");
quit;
data unique_servicetype_wother;
	input service_type $;
	datalines;
	EMERG_ROOM
	PHYS_CLIN
	HOSP_OPT
	RX_DRUG
	NURS_FAC
	IP_HOSP
	TOTAL
	;
run;
proc sql;
	create table dia2l_allclaims as
	select *, 
		case when service_type = "IP_HOSP" then inpt_spd
		when service_type = "NURS_FAC" then lt_spd
		when service_type = "PHYS_CLIN" then physclin_spd
		when service_type = "HOSP_OPT" then ot_spd
		when service_type = "RX_DRUG" then rx_spd
		when service_type = "TOTAL" then TOT_MDCD_PYMT_AMT
		else .
		end as mdcd_pymt_amt,
		case when service_type = "IP_HOSP" then inpt_clm
		when service_type = "NURS_FAC" then lt_clm
		when service_type = "PHYS_CLIN" then physclin_clm
		when service_type = "HOSP_OPT" then ot_clm
		when service_type = "RX_DRUG" then rx_clm
		when service_type = "TOTAL" then cell_n
		end as CLM_CNT
	from max_cdps, unique_servicetype_wother
	where DIA2L=1;
quit;

%collapse(in_data=dia2l_allclaims,collapse_on=st_msa,in_uniqueservicetypes =unique_servicetype_wother, orig_data=ahrf_aggre,out_data=dia2l_allclaims);
%collapse(in_data=carel_allclaims,collapse_on=st_msa, orig_data=ahrf_aggre);
%collapse(in_data=canl_allclaims,collapse_on=st_msa, orig_data=ahrf_aggre);
%collapse(in_data=prgcmp_allclaims,collapse_on=st_msa, orig_data=ahrf_aggre);
%collapse(in_data=psyl_allclaims,collapse_on=st_msa, orig_data=ahrf_aggre);
%collapse(in_data=pula_allclaims,collapse_on=st_msa, orig_data=ahrf_aggre);

proc sql;
	create table space.dia2l_claims_msacollapse as
	select *, 1 as dx_claims_only
	from space.dia2l_dxclaims_collapsed
	union corr
	select *, 0 as dx_claims_only
	from space.dia2l_allclaims_collapsed;
quit;

proc sql;
	create table space.carel_claims_msacollapse as
	select *, 1 as dx_claims_only
	from space.carel_dxclaims_collapsed
	union corr
	select *, 0 as dx_claims_only
	from space.carel_allclaims_collapsed;
quit;

proc sql;
	create table space.canl_claims_msacollapse as
	select *, 1 as dx_claims_only
	from space.canl_dxclaims_collapsed
	union corr
	select *, 0 as dx_claims_only
	from space.canl_allclaims_collapsed;
quit;

proc sql;
	create table space.prgcmp_claims_msacollapse as
	select *, 1 as dx_claims_only
	from space.prgcmp_dxclaims_collapsed
	union corr
	select *, 0 as dx_claims_only
	from space.prgcmp_allclaims_collapsed;
quit;

proc sql;
	create table space.psyl_claims_msacollapse as
	select *, 1 as dx_claims_only
	from space.psyl_dxclaims_collapsed
	union corr
	select *, 0 as dx_claims_only
	from space.psyl_allclaims_collapsed;
quit;

proc sql;
	create table space.pula_claims_msacollapse as
	select *, 1 as dx_claims_only 
		label="If = 1, row spending and utilization values include CDPS diagnosis-related claims only. If = 0, row spending and utilization values include all claims"
	from space.pula_dxclaims_collapsed
	union corr
	select *, 0 as dx_claims_only
	from space.pula_allclaims_collapsed;
quit;

proc export data=space.dia2l_claims_msacollapse
   outfile='P:\MCD-SPVR\data\NO_PII\dia2l_claims_msacollapse.csv' dbms=csv replace;
run;
proc export data=space.carel_claims_msacollapse
   outfile='P:\MCD-SPVR\data\NO_PII\carel_claims_msacollapse.csv' dbms=csv replace;
run;
proc export data=space.canl_claims_msacollapse
   outfile='P:\MCD-SPVR\data\NO_PII\canl_claims_msacollapse.csv' dbms=csv replace;
run;
proc export data=space.prgcmp_claims_msacollapse
   outfile='P:\MCD-SPVR\data\NO_PII\prgcmp_claims_msacollapse.csv' dbms=csv replace;
run;
proc export data=space.psyl_claims_msacollapse
   outfile='P:\MCD-SPVR\data\NO_PII\psyl_claims_msacollapse.csv' dbms=csv replace;
run;
proc export data=space.pula_claims_msacollapse
   outfile='P:\MCD-SPVR\data\NO_PII\pula_claims_msacollapse.csv' dbms=csv replace;
run;


/********************/
proc export data=space.dia2l_dxclaims_collapsed
   outfile='P:\MCD-SPVR\data\NO_PII\dia2l_dxclaims_collapsed.csv' dbms=csv replace;
run;
proc export data=space.carel_dxclaims_collapsed
   outfile='P:\MCD-SPVR\data\NO_PII\carel_dxclaims_collapsed.csv' dbms=csv replace;
run;
proc export data=space.canl_dxclaims_collapsed
   outfile='P:\MCD-SPVR\data\NO_PII\canl_dxclaims_collapsed.csv' dbms=csv replace;
run;
proc export data=space.prgcmp_dxclaims_collapsed
   outfile='P:\MCD-SPVR\data\NO_PII\prgcmp_dxclaims_collapsed.csv' dbms=csv replace;
run;
proc export data=space.psyl_dxclaims_collapsed
   outfile='P:\MCD-SPVR\data\NO_PII\psyl_dxclaims_collapsed.csv' dbms=csv replace;
run;
proc export data=space.pula_dxclaims_collapsed
   outfile='P:\MCD-SPVR\data\NO_PII\pula_dxclaims_collapsed.csv' dbms=csv replace;
run;
