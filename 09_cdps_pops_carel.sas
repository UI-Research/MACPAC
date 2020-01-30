/*******************************************************************************************************************/ 
/*	Purpose: Create MSA-level CDPS flag population analytic files - no MASBOE/Age category differentiators	
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak
/*	Notes: 
/*		1) Collapse macros for easier manipulation
Check:
•	Implement new managed care definition – large implication for Utah - should be fixed from base population file
•	Fix Oregon geographies -- no data - should be fixed from base population file
•	34 “ROS” geographies have no data (seems unlikely), as well as around 15 MSA areas (8 of which are in Oregon - we know what’s going on there) - should be fixed from base population file
To add:
•	Exclude those with partial benefits & add partial benefit months flag - done
•	Add Medicare wage index data - done
•	Add comorbidity elements
•	Fix comorbid missing element
/*******************************************************************************************************************/ 
%let cdps_diag = carel;
%let comorbid_cond = %str(dia2l=1 or prgcmp=1 or psyl=1 or canl=1 or pula=1);

/*Options to change*/
%macro prod();
	options obs=MAX cleanup;
	/*Log*/
	proc printto print="P:\MCD-SPVR\log\09_cdps_pops_&cdps_diag._&sysdate..lst"
	               log="P:\MCD-SPVR\log\09_cdps_pops_&cdps_diag._&sysdate..log" NEW;
	run;
%mend prod;

%macro test();	
	options obs=1000;
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
%let indata = space.pop_cdps_scores;
%let tc_value = 5000000;

/*get list of all msas for use later so that all msas are included, even if there is not data for them*/
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

	create table temp_ahrf_aggre as
	select distinct year, st_msa,
		case when substr(st_msa,4,5) = "XXXXX" then "Non-metropolitan area"
			else cbsatitle
		end as cbsatitle_fx,
		1000*sum(hos_n)/sum(pop)label = "Number of hospital beds per 1k people, 2010" as beds, 
		1000*sum(md_n)/sum(pop)label = "Number of physicians per 1k people, 2010" as md, 
		sum(poverty_d)/sum(poverty_n) label = "Rate of persons in poverty" as povrate,
		sum(unemp_d)/sum(unemp_n) label = "Unemployment rate" as urate,
		sum(ahrf_msg) as sum_ahrf_msg
	from ahrf_msa_xwalk
	group by year, st_msa, calculated cbsatitle_fx;
quit;

/*limit data to cdps pop
only cdps flag of interest
FFS and non-duals only (cell types 5-8)
children and non-elderly adults only (age_cat = 1 and 2)
full benefits only (EL_RSTRCT_BNFT_FLG_LTST in ("1","7","8","A","B"))
*/
proc sql;
	create table limited_cdpspop as
	select  *
	from &indata.
	where (&cdps_diag. = 1) and substr(age_cell,length(age_cell)-1,2) in ("05","06","07","08") and 
		age_cat in (1,2) and
		EL_RSTRCT_BNFT_FLG_LTST in ("1","7","8","A","B");
quit;

/*add diagnosis-related only claims data using 00_macro_dx_iplt_claims for ip and lt claims*/ 
%include "P:\MCD-SPVR\programs\02_Assemble_Data_Files\sandbox\00_macro_dx_iplt_claims.sas";

%pull_iplt_claims(pop_data=limited_cdpspop,filetypeshort=ip,cdps_flag=&cdps_diag.,svctype="IP_HOSP",maxtos=(1,8,12,11,16),outdata=limited_cdpspop_dxclms_ip);
%pull_iplt_claims(pop_data=limited_cdpspop,filetypeshort=lt,cdps_flag=&cdps_diag.,svctype="NURS_FAC",maxtos=(7),outdata=limited_cdpspop_dxclms_lt);

proc sql;
	title "IP";
	select count(*) as all_count from limited_cdpspop_dxclms_ip;

	select count(*) as distinct_count
	from (select distinct * from limited_cdpspop_dxclms_ip);
quit;
title;
proc sql;
	title "LT";
	select count(*) as all_count from limited_cdpspop_dxclms_lt;

	select count(*) as distinct_count
	from (select distinct * from limited_cdpspop_dxclms_lt);
quit;
title;
/*add diagnosis-related only claims data using 00_macro_dx_ot_claims for ot claims. this creates the limited_cdpspop_dxclms_ot table*/
%include "P:\MCD-SPVR\programs\02_Assemble_Data_Files\sandbox\00_macro_dx_ot_claims.sas";

proc sql;
	title "OT";
	select count(*) as all_count from limited_cdpspop_dxclms_ot;

	select count(*) as distinct_count
	from (select distinct * from limited_cdpspop_dxclms_ot);
quit;
title;

/*add the dx claims data to the limited_cdpspop data*/
%macro reshape(in_data, outlabel);
	proc sql;
		create table reshape_&outlabel. as
		select bene_id, svc_dx, state_cd,
			sum(case when service_type = "IP_HOSP" then mdcd_pymt_amt else . end) as ip_spd_dx, 
			sum(case when service_type = "NURS_FAC" then mdcd_pymt_amt else . end) as nf_spd_dx,
			sum(case when service_type = "RX_DRUG" then mdcd_pymt_amt else . end) as rx_spd_dx, 
			sum(case when service_type = "HOSP_OPT" then mdcd_pymt_amt else . end) as op_spd_dx,
			sum(case when service_type = "PHYS_CLIN" then mdcd_pymt_amt else . end) as pc_spd_dx, 
			sum(case when service_type = "EMERG_ROOM" then mdcd_pymt_amt else . end) as er_spd_dx,
			sum(mdcd_pymt_amt) as mdcd_pymt_amt_dx,

			sum(case when service_type = "IP_HOSP" then SRVC_DAYS else . end) as ip_day_dx,

			sum(case when service_type = "IP_HOSP" then clm_cnt else . end) as ip_clm_dx,
			sum(case when service_type = "NURS_FAC" then clm_cnt else . end) as nf_clm_dx,
			sum(case when service_type = "RX_DRUG" then clm_cnt else . end) as rx_clm_dx,
			sum(case when service_type = "HOSP_OPT" then clm_cnt else . end) as op_clm_dx,
			sum(case when service_type = "PHYS_CLIN" then clm_cnt else . end) as pc_clm_dx,
			sum(case when service_type = "EMERG_ROOM" then clm_cnt else . end) as er_clm_dx,

			sum(case when service_type = "IP_HOSP" then QTY_SRVC_UNITS else . end) as ip_qty_dx,
			sum(case when service_type = "NURS_FAC" then QTY_SRVC_UNITS else . end) as nf_qty_dx,
			sum(case when service_type = "RX_DRUG" then QTY_SRVC_UNITS else . end) as rx_qty_dx,
			sum(case when service_type = "HOSP_OPT" then QTY_SRVC_UNITS else . end) as op_qty_dx,
			sum(case when service_type = "PHYS_CLIN" then QTY_SRVC_UNITS else . end) as pc_qty_dx,
			sum(case when service_type = "EMERG_ROOM" then QTY_SRVC_UNITS else . end) as er_qty_dx
		from &in_data.
		where bene_id in (select bene_id from limited_cdpspop)
		group by bene_id, svc_dx, state_cd;
	quit;
%mend;

/*reshape the claims output so it can be reattached to the bene level tables */
%reshape(in_data=limited_cdpspop_dxclms_ip,outlabel=ip); 
%reshape(in_data=limited_cdpspop_dxclms_lt,outlabel=lt);
%reshape(in_data=limited_cdpspop_dxclms_ot,outlabel=ot);
data bene_svcdx_claims;
	set reshape_:;
run;
proc sql;
	select "sum("||name||") as "||name into :sum_cols separated by ", "
	from dictionary.columns
	where libname=upcase("work") and memname=upcase("bene_svcdx_claims") and name like "%_dx" and name ne "svc_dx";
quit;

%put &sum_cols.;

proc sql;
	create table claims_for_pop as
	select bene_id, svc_dx, state_cd, &sum_cols.
	from bene_svcdx_claims
	where bene_id is not null
	group by bene_id, svc_dx, state_cd;
quit;

proc sql;
	select count(*)
	from claims_for_pop;

	select count(*)
	from (select distinct * from claims_for_pop);
quit;
/*add benes that don't have claims*/
proc sql;
	create table all_pop as
	select distinct *
	from limited_cdpspop all_pop 
		left join claims_for_pop  clm
	on clm.bene_id = all_pop.bene_id and clm.state_cd=all_pop.state_cd;
quit;

/*rename all claims utilization and spending variables so that they are similar to what we will create for dx-related claims later in this program*/
/*add in additional ps level data - FFS_CLM_CNT_07, FFS_CLM_CNT_11, FFS_PYMT_AMT_07, FFS_PYMT_AMT_11 */
proc sql;
	create table pop_for_stats as
	select distinct pop.*, TOT_IP_STAY_CNT AS ip_clm, TOT_IP_DAY_CNT_STAYS as ip_day, FFS_PYMT_AMT_01 as ip_spd, (FFS_CLM_CNT_08 + FFS_CLM_CNT_12) as pc_clm, 
		(FFS_PYMT_AMT_08 + FFS_PYMT_AMT_12) as pc_spd, FFS_CLM_CNT_16 as rx_clm, FFS_PYMT_AMT_16 as rx_spd, FFS_CLM_CNT_07 as nf_clm, FFS_PYMT_AMT_07 as nf_spd,
		FFS_CLM_CNT_11 as op_clm, FFS_PYMT_AMT_11 as op_spd
	from all_pop pop left join
		(select bene_id, state_cd, FFS_CLM_CNT_07, FFS_CLM_CNT_11, FFS_PYMT_AMT_07, FFS_PYMT_AMT_11 
		from data.maxdata_ps_2012 
		where bene_id in (select bene_id from limited_cdpspop)) ps 
	on pop.bene_id = ps.bene_id and pop.state_cd=ps.state_cd;
quit;

proc sql;
	select count(*) as all_count from pop_for_stats;

	select count(*) as distinct_count
	from (select distinct * from pop_for_stats);
quit;

/*calculate the total spending and service-specific spending statistics and collapses the data to the MSA level*/
*get means of spending and CDPS and generate mult_c=(spend_c/cdps_c) by svc_dx;
/*summarizing at the ST_MSA level, not the age_cell level (as the larger analytic file is)*/
proc sql;
	create table spendavg as
	select st_msa,
		AVG(CDPS_SCORE) as cdps_c,
		AVG(mdcd_pymt_amt_dx) as mspend_dx_c,
		AVG(mdcd_pymt_amt_dx) / AVG(CDPS_SCORE) as mult_dx_c,
		AVG(tot_mdcd_pymt_amt) as mspend_c,
		AVG(tot_mdcd_pymt_amt) / AVG(CDPS_SCORE) as mult_c
	from pop_for_stats (drop=mspend_c cdps_c mult_c) /*drop previously-calculated spending elements so we can create dx-pop-specific elements*/
	group by st_msa;
quit;

*join means to individual records by 'chip','nmcd','','msg'  and 
   generate pspend_i=spend_i*mult_c and rspend_i=spend_i-pspend_i (SQL);
proc sql;
	create table pop_for_stats_res as
	select T1.*,
		(T1.tot_mdcd_pymt_amt) AS mspend_i,
		T2.mspend_c,
		T2.cdps_c,
		T2.mult_c,
		T1.CDPS_SCORE*T2.mult_c AS Pspend_i,
		(T1.tot_mdcd_pymt_amt) - T1.CDPS_SCORE*T2.mult_c AS Rspend_i,
		(T1.mdcd_pymt_amt_dx) AS mspend_dx_i,
		T2.mspend_dx_c,
		T2.mult_dx_c,
		T1.CDPS_SCORE*T2.mult_dx_c AS Pspend_dx_i,
		(T1.mdcd_pymt_amt_dx) - T1.CDPS_SCORE*T2.mult_dx_c AS Rspend_dx_i
	from pop_for_stats (drop=mspend_i mspend_c cdps_c mult_c Pspend_i Rspend_i) /*drop previously-calculated spending elements so we can create dx-pop-specific elements*/ T1 
		left join spendavg T2
	on T1.st_msa=T2.st_msa;
quit;

	/*get top coded spending vars
	replaced with a single TC value
	proc univariate data=pop_for_stats_res noprint;
		class st_msa;
		var tot_mdcd_pymt_svc_dx Rspend_i 
			ip_spd_dx pc_spd_dx rx_spd_dx nf_spd_dx op_spd_dx
			ip_spd_dx pc_spd_dx rx_spd_dx nf_spd_dx op_spd_dx;
		output out=spend_cap_&collapse_on.
		pctlpts = 99.5
		pctlpre=mdcd_spd_dx_p res_spd_dx_p ip_spd_dx_p  pc_spd_dx_p rx_spd_dx_p op_spd_dx_p nf_spd_dx_p;
	run;
	*/

proc sql;
  create table pop_for_stats_msas_spdTC AS
	select *, 
		case when tot_mdcd_pymt_amt>&tc_value. then &tc_value. else tot_mdcd_pymt_amt end as mdcd_spdTC,
		case when Rspend_i>&tc_value. then &tc_value. else Rspend_i end as res_spdTC,
		case when ip_spd>&tc_value. then &tc_value. else ip_spd end as ip_spdTC,
		case when pc_spd>&tc_value. then &tc_value. else pc_spd end as pc_spdTC,
		case when rx_spd>&tc_value. then &tc_value. else rx_spd end as rx_spdTC,
		case when op_spd>&tc_value. then &tc_value. else op_spd end as op_spdTC,
		case when nf_spd>&tc_value. then &tc_value. else nf_spd end as nf_spdTC,
		case when mdcd_pymt_amt_dx>&tc_value. then &tc_value. else mdcd_pymt_amt_dx end as mdcd_spdTC_dx,
		case when Rspend_i>&tc_value. then &tc_value. else Rspend_i end as res_spdTC_dx,
		case when ip_spd_dx>&tc_value. then &tc_value. else ip_spd_dx end as ip_spdTC_dx,
		case when pc_spd_dx>&tc_value. then &tc_value. else pc_spd_dx end as pc_spdTC_dx,
		case when rx_spd_dx>&tc_value. then &tc_value. else rx_spd_dx end as rx_spdTC_dx,
		case when op_spd_dx>&tc_value. then &tc_value. else op_spd_dx end as op_spdTC_dx,
		case when nf_spd_dx>&tc_value. then &tc_value. else nf_spd_dx end as nf_spdTC_dx
	from pop_for_stats_res;
quit;

/*get spending percentiles*/
proc univariate data=pop_for_stats_msas_spdTC noprint;
	class st_msa;
	var tot_mdcd_pymt_amt Rspend_i ip_spd pc_spd rx_spd op_spd nf_spd
		mdcd_spdTC res_spdTC ip_spdTC pc_spdTC rx_spdTC op_spdTC nf_spdTC
		ip_clm pc_clm rx_clm op_clm nf_clm

		mdcd_pymt_amt_dx Rspend_dx_i ip_spd_dx pc_spd_dx rx_spd_dx op_spd_dx nf_spd_dx
		mdcd_spdTC_dx res_spdTC_dx ip_spdTC_dx pc_spdTC_dx rx_spdTC_dx op_spdTC_dx nf_spdTC_dx
		ip_clm_dx pc_clm_dx rx_clm_dx op_clm_dx nf_clm_dx;
	output out=pctls_st_msa
	pctlpts = 10 25 50 75 90 95 99
	pctlpre=mdcd_spd_p res_spd_p ip_spd_p  pc_spd_p rx_spd_p op_spd_p nf_spd_p
			mdcd_spdTC_p res_spd_TC_p ip_spd_TC_p pc_spd_TC_p rx_spd_TC_p op_spd_TC_p nf_spd_TC_p
			ip_clm_p pc_clm_p rx_clm_p op_clm_p nf_clm_p

			mdcd_spd_dx_p res_spd_dx_p ip_spd_dx_p  pc_spd_dx_p rx_spd_dx_p op_spd_dx_p nf_spd_dx_p
			mdcd_spdTC_dx_p res_spd_TC_dx_p ip_spd_TC_dx_p pc_spd_TC_dx_p rx_spd_TC_dx_p op_spd_TC_dx_p nf_spd_TC_dx_p
			ip_clm_dx_p pc_clm_dx_p rx_clm_dx_p op_clm_dx_p nf_clm_dx_p;
run;
/*get spending stats*/
proc univariate data=pop_for_stats_msas_spdTC noprint;
	class st_msa;
	var tot_mdcd_pymt_amt mdcd_spdTC Rspend_i res_spdTC
		ip_spd pc_spd rx_spd op_spd nf_spd
		ip_spdTC pc_spdTC rx_spdTC op_spdTC nf_spdTC
		ip_clm pc_clm rx_clm op_clm nf_clm
		mdcd_pymt_amt_dx mdcd_spdTC_dx Rspend_dx_i res_spdTC_dx
		ip_spd_dx pc_spd_dx rx_spd_dx op_spd_dx nf_spd_dx
		ip_spdTC_dx pc_spdTC_dx rx_spdTC_dx op_spdTC_dx nf_spdTC_dx
		ip_clm_dx pc_clm_dx rx_clm_dx op_clm_dx nf_clm_dx;
	output out=stats_st_msa
	sum=mdcd_spd_tot mdcd_spd_tot_TC res_mdcd_spd_tot res_mdcd_spd_tot_TC
		ip_spd_tot pc_spd_tot rx_spd_tot op_spd_tot nf_spd_tot
		ip_spd_tot_TC pc_spd_tot_TC rx_spd_tot_TC op_spd_tot_TC nf_spd_tot_TC
		ip_clm_tot pc_clm_tot rx_clm_tot op_clm_tot nf_clm_tot
		mdcd_spd_dx_tot mdcd_spd_dx_tot_TC res_mdcd_spd_dx_tot res_mdcd_spd_dx_tot_TC
		ip_spd_dx_tot pc_spd_dx_tot rx_spd_dx_tot op_spd_dx_tot nf_spd_dx_tot
		ip_spd_dx_tot_TC pc_spd_dx_tot_TC rx_spd_dx_tot_TC op_spd_dx_tot_TC nf_spd_dx_tot_TC
		ip_clm_dx_tot pc_clm_dx_tot rx_clm_dx_tot op_clm_dx_tot nf_clm_dx_tot
	mean=mdcd_spd_avg mdcd_spd_avg_TC res_mdcd_spd_avg res_mdcd_spd_avg_TC
		ip_spd_avg pc_spd_avg rx_spd_avg op_spd_avg nf_spd_avg
		ip_spd_avg_TC pc_spd_avg_TC rx_spd_avg_TC op_spd_avg_TC nf_spd_avg_TC
		ip_clm_avg pc_clm_avg rx_clm_avg op_clm_avg nf_clm_avg
		mdcd_spd_dx_avg mdcd_spd_dx_avg_TC res_mdcd_spd_dx_avg res_mdcd_spd_dx_avg_TC
		ip_spd_dx_avg pc_spd_dx_avg rx_spd_dx_avg op_spd_dx_avg nf_spd_dx_avg
		ip_spd_dx_avg_TC pc_spd_dx_avg_TC rx_spd_dx_avg_TC op_spd_dx_avg_TC nf_spd_dx_avg_TC
		ip_clm_dx_avg pc_clm_dx_avg rx_clm_dx_avg op_clm_dx_avg nf_clm_dx_avg
	stdmean=mdcd_spd_se mdcd_spd_se_TC res_mdcd_spd_se res_mdcd_spd_se_TC
		ip_spd_se pc_spd_se rx_spd_se op_spd_se nf_spd_se
		ip_spd_se_TC pc_spd_se_TC rx_spd_se_TC op_spd_se_TC nf_spd_se_TC
		ip_clm_se pc_clm_se rx_clm_se op_clm_se nf_clm_se
		mdcd_spd_dx_se mdcd_spd_dx_se_TC res_mdcd_spd_dx_se res_mdcd_spd_dx_se_TC
		ip_spd_dx_se pc_spd_dx_se rx_spd_dx_se op_spd_dx_se nf_spd_dx_se
		ip_spd_dx_se_TC pc_spd_dx_se_TC rx_spd_dx_se_TC op_spd_dx_se_TC nf_spd_dx_se_TC
		ip_clm_dx_se pc_clm_dx_se rx_clm_dx_se op_clm_dx_se nf_clm_dx_se
	max=mdcd_spd_max mdcd_spd_max_TC res_mdcd_spd_max res_mdcd_spd_max_TC
		ip_spd_max pc_spd_max rx_spd_max op_spd_max nf_spd_max
		ip_spd_max_TC pc_spd_max_TC rx_spd_max_TC op_spd_max_TC nf_spd_max_TC
		ip_clm_max pc_clm_max rx_clm_max op_clm_max nf_clm_max
		mdcd_spd_dx_max mdcd_spd_dx_max_TC res_mdcd_spd_dx_max res_mdcd_spd_dx_max_TC
		ip_spd_dx_max pc_spd_dx_max rx_spd_dx_max op_spd_dx_max nf_spd_dx_max
		ip_spd_dx_max_TC pc_spd_dx_max_TC rx_spd_dx_max_TC op_spd_dx_max_TC nf_spd_dx_max_TC
		ip_clm_dx_max pc_clm_dx_max rx_clm_dx_max op_clm_dx_max nf_clm_dx_max
	;
run;

/*get basic summary stats for each MSA*/
proc sql;
	create table pop_for_stats_msalevel (rename=(cbsatitle_fx=cbsatitle)) as
		select year, st_msa,
		sum(cell_n) as cell_n,
		count(distinct bene_id) as dist_benes,
		case when substr(st_msa,4) = "XXXXX" then "Non-metro area"
			else cbsatitle
		end as cbsatitle_fx, 
		sum(prgcmp) as sum_prgcmp,
		sum(psyl) as sum_psyl,
		sum(canl) as sum_canl,
		sum(carel) as sum_carel,
		sum(pula) as sum_pula,
		sum(dia2l) as sum_dia2l,
		sum(case when &comorbid_cond then 1 end) as sum_comorbid,
		sum(case when partial_benf_mon is not null then partial_benf_mon else 0 end) as partial_benf_mon, 
		sum(mo_dual) as dual_mon, 
		sum(mo_mc) as mc_mon, 
		sum(mo_dsbl) as dis_mon, 
		sum(mo_ltss) as ltss_mon,
		sum(EL_ELGBLTY_MO_CNT) as elg_mon, 
		sum(cell_n) as cell_n,
		sum(d_cell_n) as d_cell_n, 
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
	from pop_for_stats_msas_spdTC
	group by year, st_msa, calculated cbsatitle_fx;
quit;

proc freq data=pop_for_stats_msalevel;
	tables cell_n*dist_benes/list missing;
run;
/*put overall stats into final table*/
proc sql;
	create table unique_msas as
	select distinct st_msa
	from ahrf_msa_xwalk;

	create table st_msa_collapsed (drop=st_msa rename=(new_st_msa=st_msa)) as
	select pop.*,  
	pctl.*,
	stats.*,
	ahrf.beds,  ahrf.povrate,  ahrf.sum_ahrf_msg, ahrf.md,  ahrf.urate,
	msa.*,
	coalesce(pop.st_msa, msa.st_msa) as new_st_msa
	from pop_for_stats_msalevel  pop
		full join pctls_st_msa pctl on pop.st_msa=pctl.st_msa
		full join unique_msas msa on pop.st_msa=msa.st_msa
		full join temp_ahrf_aggre ahrf on pop.st_msa= ahrf.st_msa
		full join stats_st_msa stats on pop.st_msa=stats.st_msa
	where msa.st_msa ne '';
quit;

/*add wage index data*/
/*recode medicare wage index MSAs to match our coding*/
proc sql;
	create table wageindex_2012 as
	select *,
	    case when MSA_NAME like '%NONMETROPOLITAN AREA' then catx("-",STATE,"XXXXX")
	    else catx("-",STATE,MSA_NO_)
	    end as st_msa
	from data.wageindex_2012
	where STATE ne "PR";
quit;
 /*add imputated data from Kyle Caswell*/
proc import datafile="P:\MCD-SPVR\data\raw_data\wageindmsa_v_clmsmsa_impu.csv"
	out=msa_xwalk
	dbms=csv
	replace;
	getnames=yes;
run;

proc sql;
	create table wageindex_2012_impu as
	select wag.*, wageind_st_msa, clms_st_msa, MSA_NO_, MSA_NAME, STATE,
	/*manual recodes from Kyle Caswell*/
	case when clms_st_msa = "WI-XXXXX" then 1.012
	when clms_st_msa = "WY-XXXXX" then 1.000
	else WORK
	end as WORK_fx,
	case when clms_st_msa = "WI-XXXXX" then 0.967
	when clms_st_msa = "WY-XXXXX" then 1.000
	else PE
	end as PE_fx,
	case when clms_st_msa = "WI-XXXXX" then 0.590
	when clms_st_msa = "WY-XXXXX" then 1.233
	else MPE
	end as MPE_fx
	from msa_xwalk xwalk left join wageindex_2012 (drop=VAR7) wag
	on xwalk.wageind_st_msa = wag.st_msa;
quit;

 /*join to MAX data*/
proc sql;
    create table temp_&cdps_diag. as
    select msa.*, 
        wag.WORK_fx as wageind_WORK, wag.PE_fx as wageind_PE, wag.MPE_fx as wageind_MPE
    from st_msa_collapsed msa left join wageindex_2012_impu wag
    on msa.st_msa = wag.clms_st_msa;
quit;


data space.temp_&cdps_diag.;
	set temp_&cdps_diag.;
	label 	
	st_msa ="State-MSA Code"
	cbsatitle ="CBSA Name"
	dual_mon ="Number of Person Months of Dual Eligibility"
	mc_mon ="Number of Person Months of Managed Care Enrollment"
	dis_mon ="Number of Person Months of Disability"
	ltss_mon ="Number of Person Months of LTSS Use"
	elg_mon ="Number of Person Months of Eligibility"
	cell_n ="Number of Beneficiaries"
	d_cell_n ="Number of Unique Statuses"
	died_n ="Number Dying in Year"
	cell_n="Number beneficiaries"

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
	/*Pspend_i_tot="Predicted Annual spending, from CDPS score"
	Pspend_i_dx_tot="Predicted Annual spending from diagnosis-related claims only, from CDPS score"
	Rspend_i_dx_tot="Residual annual spending from diagnosis-related claims only, from CDPS score"
	Rspend_i_tot="Residual annual spending, from CDPS scores"*/
	sum_ahrf_msg ="Sum of missing AHRF Data Flag"
	beds ="Number of hospital beds per 1k people, 2010"
	md ="Number of physicians per 1k people, 2010"
	urate ="Unemployment rate, 2012"
	povrate ="Rate of persons in poverty, 2012"
	wageind_WORK = "Medicare WORK Wage Index, 2012"
	wageind_PE = "Medicare PE Wage Index, 2012"
	wageind_MPE = "Medicare MPE Wage Index, 2012"
	partial_benf_mon = "Number of person months with partial benefits"
	sum_dia2l = "Number of benes with DIA2L flag"
	sum_pula = "Number of benes with PULA flag"
	sum_psyl =  "Number of benes with PSYL flag"
	sum_carel =  "Number of benes with CAREL flag"
	sum_canl =  "Number of benes with CANL flag"
	sum_prgcmp =  "Number of benes with PRGCMP flag"
	sum_comorbid = "Number of benes with one or more of the other 5 CDPS flags"
	;
run;

proc contents data=space.temp_&cdps_diag.;run;
proc sql;
	select sum(cell_n)
	from space.temp_&cdps_diag.;
quit;
proc contents data=out.msa_2012_02nov2019;run;


/*
proc sql;
	create table temp as 
	select *
	from st_msa_collapsed_wage_ind
	where substr(st_msa,4,8) = "XXXXX";
quit;
proc means data=st_msa_collapsed_wage_ind;run;

proc export data=space.dia2l_collapsed
   outfile='P:\MCD-SPVR\data\NO_PII\dia2l_collapse.csv' dbms=csv replace;
run;

proc export data=space.carel_collapsed
   outfile='P:\MCD-SPVR\data\NO_PII\carel_collapse.csv' dbms=csv replace;
run;
proc export data=space.canl_collapsed
   outfile='P:\MCD-SPVR\data\NO_PII\canl_collapse.csv' dbms=csv replace;
run;
proc export data=space.prgcmp_collapsed
   outfile='P:\MCD-SPVR\data\NO_PII\prgcmp_collapse.csv' dbms=csv replace;
run;
proc export data=space.psyl_collapsed
   outfile='P:\MCD-SPVR\data\NO_PII\psyl_collapse.csv' dbms=csv replace;
run;
proc export data=space.pula_collapsed
   outfile='P:\MCD-SPVR\data\NO_PII\pula_collapse.csv' dbms=csv replace;
run;

*/
