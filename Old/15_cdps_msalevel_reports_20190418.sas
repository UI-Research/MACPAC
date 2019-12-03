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
	create table max_cdps_limited as
	select a. *, b.TOT_IP_STAY_CNT AS inpt_clm, b.TOT_IP_DAY_CNT_STAYS, b.FFS_PYMT_AMT_01 as inpt_spd, (b.FFS_CLM_CNT_08 + b.FFS_CLM_CNT_12) as physclin_clm, 
		(b.FFS_PYMT_AMT_08 + b.FFS_PYMT_AMT_12) as physclin_spd, b.FFS_CLM_CNT_16 as rx_clm, b.FFS_PYMT_AMT_16 as rx_spd, FFS_CLM_CNT_07 as lt_clm, FFS_PYMT_AMT_07 as lt_spd,
		FFS_CLM_CNT_11 as ot_clm, FFS_PYMT_AMT_11 as ot_spd,
		substr(age_servicetype, length(age_servicetype)-1,2) as masboe_cat
	from space.max_cdpsscores a left join data.maxdata_ps_2012 b
	on a.bene_id = b.bene_id
	where (dia2l = 1 or carel = 1 or canl = 1 or prgcmp = 1 or psyl = 1 or pula = 1) and calculated masboe_cat in ("05","06","07","08") and a.age_cat ne 3;
quit;

proc sql;
	create table no_seniors_ip_lt as
	select *
	from SPACE.IP_LT_CDPSCLAIMS_15APR2019 a left join max_cdps_limited b/*input data file from 05_subset_claims_byDX*/
	on a.bene_id = b.bene_id;

	create table no_seniors_ot_canytx as
	select *
	from space.ot_bene_svc_canytx  a left join space.max_cdpsscores b/*input data file from 05_subset_claims_byDX*/
	on a.bene_id = b.bene_id;

	create table no_seniors_ot1_25 as
	select *
	from SPACE.OT_BENE_SVC_1_25  a left join space.max_cdps_2012 b/*input data file from 05_subset_claims_byDX*/
	on a.bene_id = b.bene_id;

	create table no_seniors_ot26_56 as
	select *
	from SPACE.OT_BENE_SVC_26_56  a left join space.max_cdpsscores b/*input data file from 05_subset_claims_byDX*/
	on a.bene_id = b.bene_id;
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
/* do i need this any more?
so every msa/service type is represented
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
*/
%let in_data=pula_dxclaims; %let collapse_on=st_msa; %let in_uniqueservicetypes=unique_servicetype; %let orig_data=ahrf_aggre; %let out_data=pula_dxclaims;

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
			sum(CLM_CNT) as clm_sum
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
		create table &out_data._msalevel (drop = bene_count_HOSP_OPT bene_count_IP_HOSP bene_count_PHYS_CLI bene_count_NURS_FAC bene_count_EMERG_RO
			dis_sum_HOSP_OPT dis_sum_IP_HOSP dis_sum_PHYS_CLI dis_sum_NURS_FAC dis_sum_EMERG_RO 
 			ltss_sum_HOSP_OPT ltss_sum_IP_HOSP ltss_sum_PHYS_CLI ltss_sum_NURS_FAC ltss_sum_EMERG_RO) as
		select *, 
			sum(bene_count_HOSP_OPT,bene_count_IP_HOSP,bene_count_PHYS_CLI, bene_count_NURS_FAC,bene_count_EMERG_RO) as sum_benes,
			sum(ltss_sum_HOSP_OPT,ltss_sum_IP_HOSP,ltss_sum_PHYS_CLI, ltss_sum_NURS_FAC,ltss_sum_EMERG_RO) as sum_ltss,
			sum(dis_sum_HOSP_OPT,dis_sum_IP_HOSP,dis_sum_PHYS_CLI, dis_sum_NURS_FAC,dis_sum_EMERG_RO) as sum_dis
		from mdcd a full join clm b on a.st_msa = b.st_msa
			full join ltss c on a.st_msa = c.st_msa
			full join dis d on a.st_msa = d.st_msa
			full join bene e on a.st_msa = e.st_msa;
	quit;
	
	
	%macro find_colnames(colname);
	%global &colname.=;
	%global &colname._formeans=;
	proc sql noprint;
		select name into :&colname. separated by " "
		from dictionary.columns
		where libname = upcase("work") and memname = upcase("&out_data._msalevel") and name like "&colname.%";
	quit;
	proc sql noprint;
		select catt(name,"=") into :&colname._formeans separated by " "
		from dictionary.columns
		where libname = upcase("work") and memname = upcase("&out_data._msalevel") and name like "&colname.%";
	quit;
	%mend;
	%find_colnames(colname=mdcd_pymt);
	%find_colnames(colname=clm_sum);
	/*get spending percentiles*/
	proc univariate data=&out_data._msalevel noprint;
		class &collapse_on.;
		var &mdcd_pymt. &clm_sum.;
		output out=pctls_&collapse_on.
		pctlpts = 10 25 50 75 90 95 99
		pctlpre=&mdcd_pymt. &clm_sum.;
	run;
	proc contents data=pctls_&collapse_on.;run;
	/*get summary stats*/
	proc means data=&out_data._msalevel  noprint;
		class &collapse_on. ;
		var &mdcd_pymt. &clm_sum.;
		output out=sumstats_&collapse_on.
		sum=&mdcd_pymt. &clm_sum.
		mean=&mdcd_pymt. &clm_sum.
		stderr=&mdcd_pymt. &clm_sum.
		max=&mdcd_pymt. &clm_sum. /autoname 
		;
	run;
	proc sql;
		/*join the beds, md, urate, and povrate to msa data*/
		create table arhf_vars_added as 
		select a.*, b.beds, b.md, b.urate, b.povrate
		from &out_data._msalevel as a left join 
			(select distinct &collapse_on., beds, md, urate, povrate from &orig_data.) as b
		on a.&collapse_on.=b.&collapse_on.;
	quit;

	/*put overall stats into final table*/
	proc sql;
		create table unique_msas as
		select distinct st_msa
		from ahrf_msa_xwalk;

		create table fintab_&collapse_on._ac (drop=st_msa service_type rename=(new_st_msa=st_msa)) as
		select a.*,  
		b.*,
		c.*,
		coalesce(a.&collapse_on.,c.&collapse_on.) as new_st_msa
		from arhf_vars_added a 
			full join pctls_&collapse_on. b on a.&collapse_on.=b.&collapse_on. 
			full join unique_msas c on a.&collapse_on.=c.&collapse_on. 
		where c.&collapse_on. ne '';
	quit;

	%let maxvars = dual_mon mc_mon dis_mon ltss_mon elg_mon cell_n d_servicetype_n died_n mas_: boe_: male: _0: _1_5: _6_18: _19_44: _45_64: _65_84: _85p: cdps spd: res:;

	*mark too-small cells missing;
	
	data space.&out_data._collapsed ;
		length st_msa $8.;
		set fintab_&collapse_on._ac;
		label 	
		cdps="Mean CDPS Score"
		cell_n ="Number of Claims"
		d_servicetype_n ="Number of Unique Statuses"
		died_n ="Number Dying in Year"
		dual_mon ="Number of Person Months of Dual Eligibility"
		mc_mon ="Number of Person Months of Managed Care Enrollment"
		dis_mon ="Number of Person Months of Disability"
		ltss_mon ="Number of Person Months of LTSS Use"
		elg_mon ="Number of Person Months of Eligibility"
		cell_n ="Number of Claims"
		bene_count = "Number of Beneficiaries"
		clm_sum_EMERG_RO = 'Number of emergency room claims'
		clm_sum_IP_HOSP = 'Number of inpatient hospital claims'
		clm_sum_HOSP_OPT = 'Number of outpatient hospital claims'
		clm_sum_NURS_FAC = 'Number of nursing facility claims'
		clm_sum_PHYS_CLI = 'Number of physician and clinic claims'
		mdcd_pymt_EMERG_RO = 'Medicaid spending for emergency room claims'
		mdcd_pymt_HOSP_OPT = 'Medicaid spending for outpatient hospital claims'
		mdcd_pymt_IP_HOSP = 'Medicaid spending for inpatient hospital claims'
		mdcd_pymt_NURS_FAC = 'Medicaid spending for nursing facility claims'
		mdcd_pymt_PHYS_CLI = 'Medicaid spending for physician and clinic claims'
		st_msa = 'State and MSA '
		sum_benes = 'Total number of beneficiaries in the MSA'
		sum_ltss = 'Total number of beneficiaries with an LTSS flag in the MSA'
		sum_dis = 'Total number of beneficiaries with a disability flag in the MSA'
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
	proc contents data=space.pula_dxclaims_collapsed;run;
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
