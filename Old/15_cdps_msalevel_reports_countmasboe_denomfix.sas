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
/*cell_n is the claims count, NOT bene count*/
proc sql;
	select count(*) as all_count, sum(cell_n) as sum_celln, count(distinct bene_id) as dist_benes
	from space.max2012_cdps_subset_wltot;
quit;
/*remove seniors per feedback from MACPAC*/
proc sql;
	create table no_seniors as
	select *
	from space.max2012_cdps_subset_wltot
	where age_cat ne 3;
quit; 



%let indata_max = no_seniors; /*input data file from 05_subset_claims_byDX*/
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
	*3 get means of spending and CDPS and generate mult_c=(spend_c/cdps_c) (by 'chip','nmcd','','msg') (SQL);
	proc sql;
		create table spendavg as
		select age_servicetype, 
			AVG(TOT_MDCD_PYMT_AMT) as mspend_c,
			AVG(CDPS_SCORE) as cdps_c,
			AVG(TOT_MDCD_PYMT_AMT) / AVG(CDPS_SCORE) as mult_c
		from &in_data.
		group by age_servicetype;
	quit;

	*4. join means to individual records by 'chip','nmcd','','msg'  and 
	   generate pspend_i=spend_i*mult_c and rspend_i=spend_i-pspend_i (SQL);
	proc sql;
		create table indata_max as
		select T1.*,
			(T1.TOT_MDCD_PYMT_AMT) AS mspend_i,
			T2.mspend_c,
			T2.cdps_c,
			T2.mult_c,
			T1.CDPS_SCORE*T2.mult_c AS Pspend_i,
			(T1.TOT_MDCD_PYMT_AMT) - T1.CDPS_SCORE*T2.mult_c AS Rspend_i
		from &in_data. T1 left join spendavg T2
		on T1.age_servicetype=T2.age_servicetype;
	quit;

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
		from indata_max a left join ahrf_msa_xwalk (drop=year) b
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
			select year, &collapse_on., sum(cell_n) as cell_n, 
			count(distinct bene_id) as bene_count,
			/*these are added in later
			sum(TOT_MDCD_PYMT_AMT) as spending,
			sum(FFS_PYMT_AMT_01) as ffspymt_01,
			sum(phys_clin_spending) as physclin, 
			sum(FFS_PYMT_AMT_16) as ffspymt_16,
			*/
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
			mean(pspend_i) as pred_mcd_spd /*,
			sum(TOT_IP_STAY_CNT) as sum_inpatient_stays,
			sum(phys_clin_claims) as sum_phys_clin_clms,
			sum(FFS_CLM_CNT_16) as sum_ffs16_clms,
			sum(ot_MSIS_TOS) as sum_outpatient_clms,
			sum(lt_MSIS_TOS) as sum_nf_clms*/
			from &indata. 
			where &cdpsflag=1
			group by year, &collapse_on.;
		quit;
%mend;

/*%collapse(indata=max_msa_2012,collapse_on=%str(age_cat, age_servicetype, st_msa,dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_dia2l, cdpsflag=dia2l);
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

/*%collapse(indata=max_msa_2012,collapse_on=%str(st_msa, dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_dia2l_noage, cdpsflag=dia2l);
%collapse(indata=max_msa_2012,collapse_on=%str(st_msa, dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_carel_noage, cdpsflag=carel);
%collapse(indata=max_msa_2012,collapse_on=%str(st_msa, dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_canl_noage, cdpsflag=canl);
%collapse(indata=max_msa_2012,collapse_on=%str(st_msa, dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_prgcmp_noage, cdpsflag=prgcmp);
%collapse(indata=max_msa_2012,collapse_on=%str(st_msa, dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_psyl_noage, cdpsflag=psyl);
%collapse(indata=max_msa_2012,collapse_on=%str(st_msa, dis_cat, dual_cat,  ltss_cat, mc_cat),outdata=msa_pula_noage, cdpsflag=pula);
*/

%collapse(indata=max_msa_2012,collapse_on=st_msa,outdata=msa_dia2l_nomasboe_noage, cdpsflag=dia2l);
%collapse(indata=max_msa_2012,collapse_on=st_msa,outdata=msa_carel_nomasboe_noage, cdpsflag=carel);
%collapse(indata=max_msa_2012,collapse_on=st_msa,outdata=msa_canl_nomasboe_noage, cdpsflag=canl);
%collapse(indata=max_msa_2012,collapse_on=st_msa,outdata=msa_prgcmp_nomasboe_noage, cdpsflag=prgcmp);
%collapse(indata=max_msa_2012,collapse_on=st_msa,outdata=msa_psyl_nomasboe_noage, cdpsflag=psyl);
%collapse(indata=max_msa_2012,collapse_on=st_msa,outdata=msa_pula_nomasboe_noage, cdpsflag=pula);

/*make final tables*/
%macro make_final_tables(indata=,indata_collapsed=,orig_data=,collapsevar=,outdata=);
proc sql;
		/*join the beds, md, urate, and povrate to msa data*/
		create table arhf_vars_added as 
		select a.*, b.beds, b.md, b.urate, b.povrate
		from &indata_collapsed as a left join 
			(select distinct &collapsevar., beds, md, urate, povrate from &orig_data.) as b
		on a.&collapsevar.=b.&collapsevar.;
	quit;
	/*spending variables*/
		/*
		Total medicaid paid amoutn (TOT_MDCD_PYMT_AMT)
		Inpatient spending (medicaid) (FFS_PYMT_AMT_01)
		Physician and clinic (combined) spending (FFS_PYMT_AMT_08+FFS_PYMT_AMT_12)phys_clin_spending
		Prescription drug spending (FFS_PYMT_AMT_16)
		Outpatient hospital spending sum_ot11_MDCD_PYMT_AMT
		Nursing facility spending  sum_lt07_MDCD_PYMT_AMT
		*/
	/*get bene level sums from claims level data*/
	proc sql; 
		create table bene_level as
		select st_msa, bene_id, 
			sum(TOT_MDCD_PYMT_AMT) as TOT_MDCD_PYMT_AMT,
			sum(Rspend_i) as Rspend_i,
			sum(FFS_PYMT_AMT_01) as FFS_PYMT_AMT_01,
			sum(phys_clin_spending) as phys_clin_spending,
			sum(FFS_PYMT_AMT_16) as FFS_PYMT_AMT_16,
			sum(MDCD_PYMT_AMT_ot11) as MDCD_PYMT_AMT_ot11,
			sum(MDCD_PYMT_AMT_lt) as MDCD_PYMT_AMT_lt,
			sum(TOT_IP_STAY_CNT) as TOT_IP_STAY_CNT,
			sum(phys_clin_claims) as phys_clin_claims,
			sum(FFS_CLM_CNT_16) as FFS_CLM_CNT_16,
			sum(MSIS_TOS_ot11) as ot_MSIS_TOS,
			sum(lt_MSIS_TOS) as lt_MSIS_TOS
		from &indata
		group by st_msa, bene_id;
	quit;

	proc univariate data=bene_level noprint;
		class &collapsevar. ;
		var TOT_MDCD_PYMT_AMT Rspend_i FFS_PYMT_AMT_01 phys_clin_spending FFS_PYMT_AMT_16 MDCD_PYMT_AMT_ot11 MDCD_PYMT_AMT_lt;
		output out=spend_pctls_&collapsevar.
		pctlpts = 10 25 50 75 90 95 99
		pctlpre=spd_p res_spd_p ffspymt01_spd_p physclin_spd_p ffspymt16_spd_p ot11_spd_p lt07_spd_p;
	run;

	proc univariate data=&indata. noprint;
		class &collapsevar.;
		var TOT_MDCD_PYMT_AMT Rspend_i FFS_PYMT_AMT_01 phys_clin_spending FFS_PYMT_AMT_16 MDCD_PYMT_AMT_ot11 MDCD_PYMT_AMT_lt;
		output out=spend_cap_&collapsevar.
		pctlpts =  99.5
		pctlpre=spd_p res_spd_p ffspymt01_spd_p physclin_spd_p ffspymt16_spd_p ot11_spd_p lt07_spd_p;
	run;

	/*put stats together*/
	proc sql;
	  create table &indata._c AS
		select a.&collapsevar., 
			a.TOT_MDCD_PYMT_AMT as mcd_spd, a.Rspend_i as res_mcd_spd, 
			FFS_PYMT_AMT_01 as ffspymt01_spd, phys_clin_spending as physclin_spd, FFS_PYMT_AMT_16 as ffspymt16_spd,
			MDCD_PYMT_AMT_ot11 as ot11_spd, MDCD_PYMT_AMT_lt as lt07_spd,
			((a.TOT_MDCD_PYMT_AMT>B.spd_p99_5)*B.spd_p99_5) as mcd_spd_TC,
			((a.Rspend_i>B.res_spd_p99_5)*B.spd_p99_5) as res_mcd_spd_TC,
			((a.FFS_PYMT_AMT_01>B.ffspymt01_spd_p99_5)*B.ffspymt01_spd_p99_5) as ffspymt01_spd_TC,
			((a.phys_clin_spending>B.physclin_spd_p99_5)*B.physclin_spd_p99_5) as physclin_spd_TC,
			((a.FFS_PYMT_AMT_16>B.ffspymt16_spd_p99_5)*B.ffspymt16_spd_p99_5) as ffspymt16_spd_TC,
			((a.MDCD_PYMT_AMT_ot11>B.ot11_spd_p99_5)*B.ot11_spd_p99_5) as ot11_spd_TC,
			((a.MDCD_PYMT_AMT_lt>B.lt07_spd_p99_5)*B.lt07_spd_p99_5) as lt07_spd_TC
		from &indata. a left join spend_cap_&collapsevar. b  
		on a.&collapsevar.=b.&collapsevar.;
	  quit;

	  /*get overall stats*/
	proc univariate data=&indata._c noprint;
		class &collapsevar. ;
		var mcd_spd mcd_spd_TC res_mcd_spd res_mcd_spd_TC ffspymt01_spd ffspymt01_spd_TC physclin_spd physclin_spd_TC ffspymt16_spd ffspymt16_spd_TC ot11_spd ot11_spd_TC lt07_spd lt07_spd_TC;
		output out=max_&collapsevar.
		sum=mcd_spd mcd_spd_TC res_mcd_spd res_mcd_spd_TC ffspymt01_spd ffspymt01_spd_TC physclin_spd physclin_spd_TC ffspymt16_spd ffspymt16_spd_TC ot11_spd ot11_spd_TC lt07_spd lt07_spd_TC
		mean=spd_avg spd_avg_TC res_spd_avg res_spd_avg_TC ffspymt01_spd_avg ffspymt01_spd_avg_TC physclin_spd_avg physclin_spd_avg_TC ffspymt16_spd_avg ffspymt16_spd_avg_TC ot11_spd_avg ot11_spd_avg_TC lt07_spd_avg lt07_spd_avg_TC
		stdmean=spd_se spd_se_TC res_spd_se res_spd_se_TC ffspymt01_spd_se ffspymt01_spd_se_TC physclin_spd_se physclin_spd_se_TC ffspymt16_spd_se ffspymt16_spd_se_TC ot11_spd_se ot11_spd_se_TC lt07_spd_se lt07_spd_se_TC
		max=spd_max spd_TC_max res_spd_max res_spd_TC_max ffspymt01_spd_max ffspymt01_spd_TC_max physclin_spd_max physclin_spd_TC_max ffspymt16_spd_max ffspymt16_spd_TC_max ot11_spd_max ot11_spd_max_TC lt07_spd_max lt07_spd_max_TC
		;
	run;

	/*count variables*/
		/*
		Inpatient stays (TOT_IP_STAY_CNT)
		Physician and clinic (combined) claims (FFS_CLM_CNT_08+FFS_CLM_CNT_12)phys_clin_claims
		Prescription drug claims(FFS_CLM_CNT_16)
		Outpatient hospital (MSIS_TOS = 7)
		Nursing facility (ot_MSIS_TOS = 11)
		*/

	proc univariate data=bene_level noprint;
		class &collapsevar. ;
		var TOT_IP_STAY_CNT phys_clin_claims FFS_CLM_CNT_16 ot_MSIS_TOS lt_MSIS_TOS;
		output out=count_pctls_&collapsevar.
		pctlpts = 10 25 50 75 90 95 99
		pctlpre=ip_sty_p physclin_clm_p ffs16_clm_p ot_tos_p lt_tos_p;
	run;

	proc sql;
		create table max_countvars_&collapsevar. as
		select &collapsevar.,
		sum(TOT_IP_STAY_CNT) as sum_TOT_IP_STAY_CNT, mean(TOT_IP_STAY_CNT) as mean_TOT_IP_STAY_CNT, std(TOT_IP_STAY_CNT) as std_TOT_IP_STAY_CNT, max(TOT_IP_STAY_CNT) as max_TOT_IP_STAY_CNT,
		sum(phys_clin_claims) as sum_phys_clin_claims, mean(phys_clin_claims) as mean_phys_clin_claims, std(phys_clin_claims) as std_phys_clin_claims, max(phys_clin_claims) as max_phys_clin_claims,
		sum(FFS_CLM_CNT_16) as sum_FFS_CLM_CNT_16, mean(FFS_CLM_CNT_16) as mean_FFS_CLM_CNT_16, std(FFS_CLM_CNT_16) as std_FFS_CLM_CNT_16, max(FFS_CLM_CNT_16) as max_FFS_CLM_CNT_16,
		sum(ot_MSIS_TOS) as sum_ot_MSIS_TOS, mean(ot_MSIS_TOS) as mean_ot_MSIS_TOS, std(ot_MSIS_TOS) as std_ot_MSIS_TOS, max(ot_MSIS_TOS) as max_ot_MSIS_TOS,
		sum(lt_MSIS_TOS) as sum_lt_MSIS_TOS, mean(lt_MSIS_TOS) as mean_lt_MSIS_TOS, std(lt_MSIS_TOS) as std_lt_MSIS_TOS, max(lt_MSIS_TOS) as max_lt_MSIS_TOS
		from bene_level
		group by &collapsevar.;

		create table unique_msas as
		select distinct st_msa
		from ahrf_msa_xwalk;

		create table all_msas_nomasboe_noage (drop=st_msa rename=(new_st_msa=st_msa)) as
		select *, 
			coalesce(a.st_msa, b.st_msa) as new_st_msa
		from max_countvars_&collapsevar. a 
		full join unique_msas b
		on a.&collapsevar. = b.&collapsevar.
		where b.&collapsevar. ne '';
	quit;
	/*put overall stats into final table*/
	proc sql;
		create table fintab_&collapsevar._ac as
		select a.*,  
		b.*,
		c.*,
		d.*,
		e.*
		from arhf_vars_added a 
			full join max_&collapsevar. b on a.&collapsevar.=b.&collapsevar.
			full join spend_pctls_&collapsevar. c on a.&collapsevar.=c.&collapsevar.
			full join count_pctls_&collapsevar. d on a.&collapsevar.=d.&collapsevar.
			full join all_msas_nomasboe_noage e on a.&collapsevar.=e.&collapsevar.
		where e.&collapsevar. ne ''; 
	quit;

	%let maxvars = dual_mon mc_mon dis_mon ltss_mon elg_mon cell_n d_servicetype_n died_n mas_: boe_: male: _0: _1_5: _6_18: _19_44: _45_64: _65_84: _85p: cdps no_cdps_conds pred_mcd_spd mcd_spd_tot spd: res:;

	*mark too-small cells missing;
	data &outdata. ;
		length st_msa $8.;
		set fintab_&collapsevar._ac;
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
			pred_mcd_spd="Predicted Annual spending, from CDPS score"
			beds ="Number of hospital beds per 1k people, 2010"
			md ="Number of physicians per 1k people, 2010"
			urate ="Unemployment rate, 2012"
			povrate ="Rate of persons in poverty, 2012"
			mcd_spd_tot ="Total Annual Spending"
			spd_avg ="Mean Annual Spending "
			mcd_spd = "Annual Spending"
			spd_se ="Standard Error of Mean Annual Spending"
			spd_avg_tc ="Mean Annual Spending  (Top Coded)"
			spd_se_tc ="Standard Error of Mean Annual Spending (Top Coded)"
			spd_tc_max ="Maximum Annual Spending  (Top Coded)"
			spd_p10 ="10th Percentile of Annual Spending"
			spd_p25 ="25th Percentile of Annual Spending"
			spd_p50 ="50th Percentile of Annual Spending"
			spd_p75 ="75th Percentile of Annual Spending"
			spd_p90 ="90th Percentile of Annual Spending"
			spd_p95 ="95th Percentile of Annual Spending"
			spd_p99 ="99th Percentile of Annual Spending"
			spd_max ="Maximum Annual Spending "
			res_mcd_spd_tot ="Total Annual Spending Residual"
			res_spd_avg ="Mean Annual Spending Residual "
			res_spd_se ="Standard Error of Mean Annual Spending Residual"
			res_spd_avg_tc ="Mean Annual Spending Residual  (Top Coded)"
			res_spd_se_tc ="Standard Error of Mean Annual Spending Residual (Top Coded)"
			res_spd_tc_max ="Maximum Annual Spending Residual  (Top Coded)"
			res_spd_p10 ="10th Percentile of Annual Spending Residual"
			res_spd_p25 ="25th Percentile of Annual Spending Residual"
			res_spd_p50 ="50th Percentile of Annual Spending Residual"
			res_spd_p75 ="75th Percentile of Annual Spending Residual"
			res_spd_p90 ="90th Percentile of Annual Spending Residual"
			res_spd_p95 ="95th Percentile of Annual Spending Residual"
			res_spd_p99 ="99th Percentile of Annual Spending Residual"
			res_spd_max ="Maximum Annual Spending Residual "
			ffspymt01_spd = "Inpatient spending (Medicaid)"
			ffspymt01_spd_avg ="Mean inpatient spending "
			ffspymt01_spd_se ="Standard Error of Mean inpatient spending"
			ffspymt01_spd_avg_tc ="Mean inpatient spending  (Top Coded)"
			ffspymt01_spd_se_tc ="Standard Error of Mean inpatient spending (Top Coded)"
			ffspymt01_spd_tc_max ="Maximum inpatient spending  (Top Coded)"
			ffspymt01_spd_p10 ="10th Percentile of inpatient spending"
			ffspymt01_spd_p25 ="25th Percentile of inpatient spending"
			ffspymt01_spd_p50 ="50th Percentile of inpatient spending"
			ffspymt01_spd_p75 ="75th Percentile of inpatient spending"
			ffspymt01_spd_p90 ="90th Percentile of inpatient spending"
			ffspymt01_spd_p95 ="95th Percentile of inpatient spending"
			ffspymt01_spd_p99 ="99th Percentile of inpatient spending"
			ffspymt01_spd_max ="Maximum inpatient spending "
			physclin_spd = "Physician and clinic (combined) spending"
			physclin_spd_avg ="Mean physician and clinic (combined) spending "
			physclin_spd_se ="Standard Error of Mean physician and clinic (combined) spending"
			physclin_spd_avg_tc ="Mean physician and clinic (combined) spending  (Top Coded)"
			physclin_spd_se_tc ="Standard Error of Mean physician and clinic (combined) spending (Top Coded)"
			physclin_spd_tc_max ="Maximum physician and clinic (combined) spending  (Top Coded)"
			physclin_spd_p10 ="10th Percentile of physician and clinic (combined) spending"
			physclin_spd_p25 ="25th Percentile of physician and clinic (combined) spending"
			physclin_spd_p50 ="50th Percentile of physician and clinic (combined) spending"
			physclin_spd_p75 ="75th Percentile of physician and clinic (combined) spending"
			physclin_spd_p90 ="90th Percentile of physician and clinic (combined) spending"
			physclin_spd_p95 ="95th Percentile of physician and clinic (combined) spending"
			physclin_spd_p99 ="99th Percentile of physician and clinic (combined) spending"
			physclin_spd_max ="Maximum physician and clinic (combined) spending "
			ffspymt16_spd = "Prescription drug spending"
			ffspymt16_spd_avg ="Mean prescription drug spending "
			ffspymt16_spd_se ="Standard Error of Mean prescription drug spending"
			ffspymt16_spd_avg_tc ="Mean prescription drug spending  (Top Coded)"
			ffspymt16_spd_se_tc ="Standard Error of Mean prescription drug spending (Top Coded)"
			ffspymt16_spd_tc_max ="Maximum prescription drug spending  (Top Coded)"
			ffspymt16_spd_p10 ="10th Percentile of prescription drug spending"
			ffspymt16_spd_p25 ="25th Percentile of prescription drug spending"
			ffspymt16_spd_p50 ="50th Percentile of prescription drug spending"
			ffspymt16_spd_p75 ="75th Percentile of prescription drug spending"
			ffspymt16_spd_p90 ="90th Percentile of prescription drug spending"
			ffspymt16_spd_p95 ="95th Percentile of prescription drug spending"
			ffspymt16_spd_p99 ="99th Percentile of prescription drug spending"
			ffspymt16_spd_max ="Maximum prescription drug spending"
			lt07_spd = "Nursing facility  spending"
			lt07_spd_avg ="Mean nursing facility  spending "
			lt07_spd_se ="Standard Error of Mean nursing facility  spending"
			lt07_spd_avg_tc ="Mean nursing facility  spending  (Top Coded)"
			lt07_spd_se_tc ="Standard Error of Mean nursing facility  spending (Top Coded)"
			lt07_spd_max_TC ="Maximum nursing facility  spending  (Top Coded)"
			lt07_spd_p10 ="10th Percentile of nursing facility  spending"
			lt07_spd_p25 ="25th Percentile of nursing facility  spending"
			lt07_spd_p50 ="50th Percentile of nursing facility  spending"
			lt07_spd_p75 ="75th Percentile of nursing facility  spending"
			lt07_spd_p90 ="90th Percentile of nursing facility  spending"
			lt07_spd_p95 ="95th Percentile of nursing facility  spending"
			lt07_spd_p99 ="99th Percentile of nursing facility  spending"
			lt07_spd_max ="Maximum nursing facility spending"
			ot11_spd = "Outpatient hospital spending"
			ot11_spd_avg ="Mean outpatient hospital  spending "
			ot11_spd_se ="Standard Error of Mean outpatient hospital  spending"
			ot11_spd_avg_tc ="Mean outpatient hospital  spending  (Top Coded)"
			ot11_spd_se_tc ="Standard Error of Mean outpatient hospital  spending (Top Coded)"
			ot11_spd_max_TC ="Maximum outpatient hospital  spending  (Top Coded)"
			ot11_spd_p10 ="10th Percentile of outpatient hospital  spending"
			ot11_spd_p25 ="25th Percentile of outpatient hospital  spending"
			ot11_spd_p50 ="50th Percentile of outpatient hospital  spending"
			ot11_spd_p75 ="75th Percentile of outpatient hospital  spending"
			ot11_spd_p90 ="90th Percentile of outpatient hospital  spending"
			ot11_spd_p95 ="95th Percentile of outpatient hospital  spending"
			ot11_spd_p99 ="99th Percentile of outpatient hospital  spending"
			ot11_spd_max ="Maximum outpatient hospital spending"
			sum_TOT_IP_STAY_CNT = "Sum inpatient stays"
			mean_TOT_IP_STAY_CNT = "Mean inpatient stays"
			std_TOT_IP_STAY_CNT = "Standard error of the mean inpatient stays"
			max_TOT_IP_STAY_CNT = "Maximum inpatient stays"
			sum_phys_clin_claims = "Sum physician and clinic (combined) claims"
			mean_phys_clin_claims = "Mean physician and clinic (combined) claims"
			std_phys_clin_claims = "Standard error of the mean physician and clinic (combined) claims"
			max_phys_clin_claims = "Maximum physician and clinic (combined) claims"
			sum_FFS_CLM_CNT_16 = "Sum prescription drug claims"
			mean_FFS_CLM_CNT_16 = "Meam prescription drug claims"
			std_FFS_CLM_CNT_16 = "Standard error of the mean prescription drug claims"
			max_FFS_CLM_CNT_16 = "Maximum prescription drug claims"
			sum_ot_MSIS_TOS = "Sum outpatient hospital claims"
			mean_ot_MSIS_TOS = "Mean outpatient hospital claims"
			std_ot_MSIS_TOS = "Standard error of the mean outpatient hospital claims"
			max_ot_MSIS_TOS = "Maximum outpatient hospital claims"
			sum_lt_MSIS_TOS = "Sum nursing facility claims"
			mean_lt_MSIS_TOS = "Mean nursing facility claims"
			std_lt_MSIS_TOS = "Standard error of the mean nursing facility claims"
			max_lt_MSIS_TOS = "Maximum nursing facility claims"
			;
	
			array all_cells {*} &maxvars.; 
			if cell_n<11 then do;
				do i=1 to dim(all_cells);
					all_cells(i)=.;
				end;
			end;
			drop i;
	run;
%mend;

/*
proc sql;
	create table dia2l_cdps_benes as
	select *
	from max_2012_msa_join
	where bene_id in (select distinct bene_id from &indata_max) and svc_dx = "DIAB";
quit;

proc sql;
	create table carel_cdps_benes as
	select *
	from max_2012_msa_join
	where svc_dx = "HYPT" and bene_id in (select distinct bene_id from &indata_max);
quit;
proc sql;
	create table canl_cdps_benes as
	select *
	from max_2012_msa_join
	where svc_dx = "CANC" and bene_id in (select distinct bene_id from &indata_max);
quit;
proc sql;
	create table prgcmp_cdps_benes as
	select *
	from max_2012_msa_join
	where svc_dx = "PREG" and bene_id in (select distinct bene_id from &indata_max);
quit;
proc sql;
	create table psyl_cdps_benes as
	select *
	from max_2012_msa_join
	where svc_dx = "PSYC" and bene_id in (select distinct bene_id from &indata_max);
quit;
proc sql;
	create table pula_cdps_benes as
	select *
	from max_2012_msa_join
	where svc_dx = "ASTH" and bene_id in (select distinct bene_id from &indata_max);
quit;
*/
%make_final_tables(indata=dia2l_cdps_benes,indata_collapsed=msa_dia2l_nomasboe_noage,orig_data=ahrf_aggre, collapsevar=st_msa,outdata=space.msa_dia2l_nomasboe_noage);
proc sql; drop table MAX_2012_MSA_JOIN_C; quit;
%make_final_tables(indata=carel_cdps_benes,indata_collapsed=msa_carel_nomasboe_noage,orig_data=ahrf_aggre, collapsevar=st_msa,outdata=space.msa_carel_nomasboe_noage);
proc sql; drop table MAX_2012_MSA_JOIN_C; quit;
%make_final_tables(indata=canl_cdps_benes,indata_collapsed=msa_canl_nomasboe_noage,orig_data=ahrf_aggre, collapsevar=st_msa,outdata=space.msa_canl_nomasboe_noage);
proc sql; drop table MAX_2012_MSA_JOIN_C; quit;
%make_final_tables(indata=prgcmp_cdps_benes,indata_collapsed=msa_prgcmp_nomasboe_noage,orig_data=ahrf_aggre, collapsevar=st_msa,outdata=space.msa_prgcmp_nomasboe_noage);
proc sql; drop table MAX_2012_MSA_JOIN_C; quit;
%make_final_tables(indata=psyl_cdps_benes,indata_collapsed=msa_psyl_nomasboe_noage,orig_data=ahrf_aggre, collapsevar=st_msa,outdata=space.msa_psyl_nomasboe_noage);
proc sql; drop table MAX_2012_MSA_JOIN_C; quit;
%make_final_tables(indata=pula_cdps_benes,indata_collapsed=msa_pula_nomasboe_noage,orig_data=ahrf_aggre, collapsevar=st_msa,outdata=space.msa_pula_nomasboe_noage);

proc export data=space.msa_dia2l_nomasboe_noage
   outfile='P:\MCD-SPVR\data\NO_PII\msa_dia2l_nomasboe_noage.csv' dbms=csv;
run;
proc export data=space.msa_carel_nomasboe_noage
   outfile='P:\MCD-SPVR\data\NO_PII\msa_carel_nomasboe_noage.csv' dbms=csv;
run;
proc export data=space.msa_canl_nomasboe_noage
   outfile='P:\MCD-SPVR\data\NO_PII\msa_canl_nomasboe_noage.csv' dbms=csv;
run;
proc export data=space.msa_prgcmp_nomasboe_noage
   outfile='P:\MCD-SPVR\data\NO_PII\msa_prgcmp_nomasboe_noage.csv' dbms=csv;
run;
proc export data=space.msa_psyl_nomasboe_noage
   outfile='P:\MCD-SPVR\data\NO_PII\msa_psyl_nomasboe_noage.csv' dbms=csv;
run;
proc export data=space.msa_pula_nomasboe_noage
   outfile='P:\MCD-SPVR\data\NO_PII\msa_pula_nomasboe_noage.csv' dbms=csv;
run;
*/
%macro sum_tab(cdps_msa_dat,variable,largecelln,outdata);
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
	/*proc sql;
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
	quit;/*moved up to 'make final tables' section */ 

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

%sum_tab(cdps_msa_dat=msa_dia2l,variable=cell_n,largecelln=100,outdata=dia2l_n_tab)
%sum_tab_nomasboe_noage(cdps_msa_dat=msa_dia2l_nomasboe_noage,variable=cell_n,largecelln=100,outdata=dia2l_n_tab_nomasboe_noage)

%sum_tab(cdps_msa_dat=msa_carel,variable=cell_n,largecelln=100,outdata=carel_n_tab)
%sum_tab_nomasboe_noage(cdps_msa_dat=msa_carel_nomasboe_noage,variable=cell_n,largecelln=100,outdata=carel_n_tab_nomasboe_noage)

%sum_tab(cdps_msa_dat=msa_canl,variable=cell_n,largecelln=100,outdata=canl_n_tab)
%sum_tab_nomasboe_noage(cdps_msa_dat=msa_canl_nomasboe_noage,variable=cell_n,largecelln=100,outdata=canl_n_tab_nomasboe_noage)

%sum_tab(cdps_msa_dat=msa_prgcmp,variable=cell_n,largecelln=100,outdata=prgcmp_n_tab)
%sum_tab_nomasboe_noage(cdps_msa_dat=msa_prgcmp_nomasboe_noage,variable=cell_n,largecelln=100,outdata=prgcmp_n_tab_nomasboe_noage)

%sum_tab(cdps_msa_dat=msa_psyl,variable=cell_n,largecelln=100,outdata=psyl_n_tab)
%sum_tab_nomasboe(cdps_msa_dat=msa_psyl,variable=cell_n,largecelln=100,outdata=psyl_n_tab_nomasboe)
%sum_tab_nomasboe_noage(cdps_msa_dat=msa_psyl_nomasboe_noage,variable=cell_n,largecelln=100,outdata=psyl_n_tab_nomasboe_noage)

%sum_tab(cdps_msa_dat=msa_pula,variable=cell_n,largecelln=100,outdata=pula_n_tab)
%sum_tab_nomasboe(cdps_msa_dat=msa_pula,variable=cell_n,largecelln=100,outdata=pula_n_tab_nomasboe)
%sum_tab_nomasboe_noage(cdps_msa_dat=msa_pula_nomasboe_noage,variable=cell_n,largecelln=100,outdata=pula_n_tab_nomasboe_noage)

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
%macro tabs_tgthr_nomasboeage_wcnts(n_dat, n_dat_nomasboe_noage);
	proc sql; 
		select avg, p1, p5, p10, p25, p50, p75, p90, p95, p99, min, max, n_st_msas, sum_benes format=COMMA8., pct_lg_msas,
			(select sum(sum_benes) from &n_dat. where substr(new_age_servicetype,length(new_age_servicetype)-1,2) = '05') as sum_service05 format=COMMA8. label = "Medicaid Only, Fee-For-Service, Disability, LTSS",
			(select sum(sum_benes) from &n_dat. where substr(new_age_servicetype,length(new_age_servicetype)-1,2) = '06') as sum_service06 format=COMMA8. label = "Medicaid Only, Fee-For-Service, Disability, No LTSS",
			(select sum(sum_benes) from &n_dat. where substr(new_age_servicetype,length(new_age_servicetype)-1,2) = '07') as sum_service07 format=COMMA8. label = "Medicaid Only, Fee-For-Service, No Disability, LTSS",
			(select sum(sum_benes) from &n_dat. where substr(new_age_servicetype,length(new_age_servicetype)-1,2) = '08') as sum_service08 format=COMMA8. label = "Medicaid Only, Fee-For-Service, No Disability, No LTSS"
		from &n_dat_nomasboe_noage.;
	quit;
%mend;
%macro tabs_tgthr_nomasboe_wcnts(n_dat, n_dat_nomasboe);
	proc sql;
		create table agecat_counts as
		select agecat, substr(new_age_servicetype,length(new_age_servicetype)-1,2) as servicetype, sum(sum_benes) as sum_benes
		from &n_dat.
		group by agecat, substr(new_age_servicetype,length(new_age_servicetype)-1,2);
	quit;
	proc sql; 
		select 
			case 
				when new_age_cat = 1 then "Children"
				when new_age_cat = 2 then "Adults"
				when new_age_cat = 3 then "Seniors"
			end as age_cat label = "Age Category",
			avg, p1, p5, p10, p25, p50, p75, p90, p95, p99, min, max, n_st_msas, sum_benes format=COMMA8., pct_lg_msas,
			case
				when new_age_Cat = 1 then (select sum(sum_benes) from agecat_counts where servicetype = '05' and agecat=1)
				when new_age_Cat = 2 then (select sum(sum_benes) from agecat_counts where servicetype = '05' and agecat=2)
				when new_age_Cat = 3 then (select sum(sum_benes) from agecat_counts where servicetype = '05' and agecat=3)
			end as sum_service05 format=COMMA8. label = "Medicaid Only, Fee-For-Service, Disability, LTSS",
			case
				when new_age_Cat = 1 then (select sum(sum_benes) from agecat_counts where servicetype = '06' and agecat=1)
				when new_age_Cat = 2 then (select sum(sum_benes) from agecat_counts where servicetype = '06' and agecat=2)
				when new_age_Cat = 3 then (select sum(sum_benes) from agecat_counts where servicetype = '06' and agecat=3)
			end as sum_service05 format=COMMA8. label = "Medicaid Only, Fee-For-Service, Disability, No LTSS",
			case
				when new_age_Cat = 1 then (select sum(sum_benes) from agecat_counts where servicetype = '07' and agecat=1)
				when new_age_Cat = 2 then (select sum(sum_benes) from agecat_counts where servicetype = '07' and agecat=2)
				when new_age_Cat = 3 then (select sum(sum_benes) from agecat_counts where servicetype = '07' and agecat=3)
			end as sum_service05 format=COMMA8. label = "Medicaid Only, Fee-For-Service, No Disability, LTSS",
			case
				when new_age_Cat = 1 then (select sum(sum_benes) from agecat_counts where servicetype = '08' and agecat=1)
				when new_age_Cat = 2 then (select sum(sum_benes) from agecat_counts where servicetype = '08' and agecat=2)
				when new_age_Cat = 3 then (select sum(sum_benes) from agecat_counts where servicetype = '08' and agecat=3)
			end as sum_service05 format=COMMA8. label = "Medicaid Only, Fee-For-Service, No Disability, No LTSS"
		from &n_dat_nomasboe.;
	quit;
%mend;

	ods excel file="&report_folder.\msa_cdps_subsetclaims_&space_name..xlsx";
	ods excel options(sheet_name="All CDPS Flag Groups" sheet_interval="none" absolute_column_width='20,20,20,25,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10' title_footnote_nobreak="no");
		proc odstext; p "Diabetes" / style=[color=black font_weight=bold]; run;
		%tabs_tgthr_nomasboeage_wcnts(n_dat=dia2l_n_tab, n_dat_nomasboe_noage = dia2l_n_tab_nomasboe_noage);
		proc odstext; p "Hypertension" / style=[color=black font_weight=bold]; run;
		%tabs_tgthr_nomasboeage_wcnts(n_dat=carel_n_tab, n_dat_nomasboe_noage = carel_n_tab_nomasboe_noage);
		proc odstext; p "Colon, prostate, cervical cancers"  / style=[color=black font_weight=bold]; run;
		%tabs_tgthr_nomasboeage_wcnts(n_dat=canl_n_tab, n_dat_nomasboe_noage = canl_n_tab_nomasboe_noage);
		proc odstext; p "Completed Pregnancy" / style=[color=black font_weight=bold]; run;
		%tabs_tgthr_nomasboeage_wcnts(n_dat=prgcmp_n_tab, n_dat_nomasboe_noage = prgcmp_n_tab_nomasboe_noage);
		proc odstext; p "Anxiety, Depression, Phobia" / style=[color=black font_weight=bold]; run;
		%tabs_tgthr_nomasboeage_wcnts(n_dat=psyl_n_tab, n_dat_nomasboe_noage = psyl_n_tab_nomasboe_noage);
		proc odstext; p "Asthma" / style=[color=black font_weight=bold]; run;
		%tabs_tgthr_nomasboeage_wcnts(n_dat=pula_n_tab, n_dat_nomasboe_noage = pula_n_tab_nomasboe_noage);
proc odstext; p "*Large MSAs have >= 100 benes"; run;
	ods excel close;

