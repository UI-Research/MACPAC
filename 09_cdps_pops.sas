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
/*******************************************************************************************************************/ 
/*Options to change*/
%macro prod();
	options obs=MAX cleanup;
	/*Log*/
	proc printto print="P:\MCD-SPVR\log\09_cdps_pops&sysdate..lst"
	               log="P:\MCD-SPVR\log\09_cdps_pops&sysdate..log" NEW;
	run;
%mend prod;

%macro test();	
	options obs=100;
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

%let cdps_diag = dia2l;

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

proc freq data=limited_cdpspop;
	tables EL_RSTRCT_BNFT_FLG_LTST/list missing;
run;

/*add diagnosis-related only claims data using 00_macro_dx_iplt_claims for ip and lt claims*/ 
%include "P:\MCD-SPVR\programs\02_Assemble_Data_Files\sandbox\00_macro_dx_iplt_claims.sas";

%pull_iplt_claims(pop_data=limited_cdpspop,filetypeshort=ip,cdps_flag=%nrquote(&dia2l.),svctype="IP_HOSP",maxtos=(1,8,12,11,16),outdata=limited_cdpspop_dxclms_ip);
%pull_iplt_claims(pop_data=limited_cdpspop,filetypeshort=lt,cdps_flag=%nrquote(&dia2l.),svctype="NURS_FAC",maxtos=(7),outdata=limited_cdpspop_dxclms_lt);

proc sql;
	select count(*) as all_count from limited_cdpspop_dxclms_ip;

	select count(*) as distinct_count
	from (select distinct * from limited_cdpspop_dxclms_ip);
quit;

proc sql;
	select count(*) as all_count from limited_cdpspop_dxclms_lt;

	select count(*) as distinct_count
	from (select distinct * from limited_cdpspop_dxclms_lt);
quit;

/*add diagnosis-related only claims data using 00_macro_dx_ot_claims for ot claims. this creates the limited_cdpspop_dxclms_ot table*/
%include "P:\MCD-SPVR\programs\02_Assemble_Data_Files\sandbox\00_macro_dx_ot_claims.sas";
%pull_ot_claims(pop_data=limited_cdpspop,cdps_flag=%nrquote(&dia2l.),outdata=limited_cdpspop_dxclms_ot);

proc sql;
	select count(*) as all_count from limited_cdpspop_dxclms_ot;

	select count(*) as distinct_count
	from (select distinct * from limited_cdpspop_dxclms_ot);
quit;

/*add the dx claims data to the limited_cdpspop data*/
%macro reshape(in_data, outlabel);
	proc sql;
		create table reshaped as
		select bene_id, svc_dx,
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
		group by bene_id, svc_dx;
	
		create table reshape_&outlabel. as
		select *
		from reshaped clm inner join limited_cdpspop pop
		on clm.bene_id = pop.bene_id;
%mend;

/*reshape the claims output so it can be reattached to the bene level tables */
%reshape(in_data=limited_cdpspop_dxclms_ip,outlabel=ip); 
%reshape(in_data=limited_cdpspop_dxclms_lt,outlabel=lt);
%reshape(in_data=limited_cdpspop_dxclms_ot,outlabel=ot);

data bene_svcdx_claims;
	set reshape_:;
run;

/*rename all claims utilization and spending variables so that they are similar to what we will create for dx-related claims later in this program*/
/*add in additional ps level data - FFS_CLM_CNT_07, FFS_CLM_CNT_11, FFS_PYMT_AMT_07, FFS_PYMT_AMT_11 */
proc sql;
	create table pop_for_stats as
	select distinct pop.*, pop.TOT_IP_STAY_CNT AS ip_clm, pop.TOT_IP_DAY_CNT_STAYS as ip_day, pop.FFS_PYMT_AMT_01 as ip_spd, (pop.FFS_CLM_CNT_08 + pop.FFS_CLM_CNT_12) as pc_clm, 
		(pop.FFS_PYMT_AMT_08 + pop.FFS_PYMT_AMT_12) as pc_spd, pop.FFS_CLM_CNT_16 as rx_clm, pop.FFS_PYMT_AMT_16 as rx_spd, FFS_CLM_CNT_07 as nf_clm, FFS_PYMT_AMT_07 as nf_spd,
		FFS_CLM_CNT_11 as op_clm, FFS_PYMT_AMT_11 as op_spd
	from bene_svcdx_claims pop left join 
		(select bene_id, FFS_CLM_CNT_07, FFS_CLM_CNT_11, FFS_PYMT_AMT_07, FFS_PYMT_AMT_11 
		from data.maxdata_ps_2012 
		where bene_id in (select bene_id from bene_svcdx_claims)) ps 
	on pop.bene_id = ps.bene_id;
quit;

proc sql;
	select count(*) as all_count from pop_for_stats;

	select count(*) as distinct_count
	from (select distinct * from pop_for_stats);
quit;
proc contents data=pop_for_stats;run;
/*calculate the total spending and service-specific spending statistics and collapses the data to the MSA level*/
*get means of spending and CDPS and generate mult_c=(spend_c/cdps_c) by svc_dx;
proc sql;
	create table spendavg as
	select age_cell,
		AVG(CDPS_SCORE) as cdps_c,
		AVG(mdcd_pymt_amt_dx) as mspend_dx_c,
		AVG(mdcd_pymt_amt_dx) / AVG(CDPS_SCORE) as mult_dx_c,
		AVG(tot_mdcd_pymt_amt) as mspend_c,
		AVG(tot_mdcd_pymt_amt) / AVG(CDPS_SCORE) as mult_c
	from pop_for_stats (drop=mspend_c cdps_c mult_c) /*drop previously-calculated spending elements so we can create dx-pop-specific elements*/
	group by age_cell;
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
	on T1.age_cell=T2.age_cell;
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

/*get basic summary stats for each MSA*/
proc sql;
	create table pop_for_stats_msalevel as
		select year, age_cell, age_cat, st_msa, dis_cat, dual_cat, foster_cat, ltss_cat, mc_cat,
		count(distinct bene_id) as dist_bene_count,
		case when substr(st_msa,4) = "XXXXX" then "Non-metro area"
			else cbsatitle
		end as cbsatitle_fx, 
		sum(partial_benf_mon) as partial_benf_mon, 
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
		mean(NOCDPS) as no_cdps_conds,	
		sum(cdps_c) as cdps_c_sum, 
		/*all claims variables*/
		sum(Pspend_i) as Pspend_i_sum, 
		sum(Rspend_i) as Rspend_i_sum, 
		sum(mspend_c) as mspend_c_sum, 
		sum(mspend_i) as mspend_i_sum, 
		sum(mult_c) as mult_c_sum, 
		sum(tot_mdcd_pymt_amt) as mdcd_spd_sum,
		sum(mdcd_spdTC) as mdcd_spdTC_sum, 
		sum(res_spdTC) as res_spdTC_sum, 
		/*er and qty elements not calculated for all claims*/
		sum(ip_clm) as ip_clm_sum, 
		sum(ip_day) as ip_day_sum, 
		sum(ip_spd) as ip_spd_sum, 
		sum(ip_spdTC) as ip_spdTC_sum, 
		sum(nf_clm) as nf_clm_sum, 
		sum(nf_spd) as nf_spd_sum, 
		sum(nf_spdTC) as nf_spdTC_sum, 
		sum(op_clm) as op_clm_sum, 
		sum(op_spd) as op_spd_sum, 
		sum(op_spdTC) as op_spdTC_sum, 
		sum(pc_clm) as pc_clm_sum, 
		sum(pc_spd) as pc_spd_sum, 
		sum(pc_spdTC) as pc_spdTC_sum, 
		sum(rx_clm) as rx_clm_sum, 
		sum(rx_spd) as rx_spd_sum, 
		sum(rx_spdTC) as rx_spdTC_sum,
		/*dx-related claims variables*/
		sum(Pspend_dx_i) as Pspend_i_dx_sum, 
		sum(Rspend_dx_i) as Rspend_i_dx_sum, 
		sum(mspend_dx_c) as mspend_c_dx_sum, 
		sum(mspend_dx_i) as mspend_i_dx_sum, 
		sum(mult_dx_c) as mult_c_dx_sum, 
		sum(mdcd_pymt_amt_dx) as mdcd_spd_dx_sum,
		sum(mdcd_spdTC_dx) as mdcd_spdTC_dx_sum, 
		sum(res_spdTC_dx) as res_spdTC_dx_sum, 
		sum(er_clm_dx) as er_clm_dx_sum, 
		sum(er_qty_dx) as er_qty_dx_sum, 
		sum(er_spd_dx) as er_spd_dx_sum, 
		sum(ip_clm_dx) as ip_clm_dx_sum, 
		sum(ip_day_dx) as ip_day_dx_sum, 
		sum(ip_qty_dx) as ip_qty_dx_sum, 
		sum(ip_spd_dx) as ip_spd_dx_sum, 
		sum(ip_spdTC_dx) as ip_spdTC_dx_sum, 
		sum(nf_clm_dx) as nf_clm_dx_sum, 
		sum(nf_qty_dx) as nf_qty_dx_sum, 
		sum(nf_spd_dx) as nf_spd_dx_sum, 
		sum(nf_spdTC_dx) as nf_spdTC_dx_sum, 
		sum(op_clm_dx) as op_clm_dx_sum, 
		sum(op_qty_dx) as op_qty_dx_sum, 
		sum(op_spd_dx) as op_spd_dx_sum, 
		sum(op_spdTC_dx) as op_spdTC_dx_sum, 
		sum(pc_clm_dx) as pc_clm_dx_sum, 
		sum(pc_qty_dx) as pc_qty_dx_sum, 
		sum(pc_spd_dx) as pc_spd_dx_sum, 
		sum(pc_spdTC_dx) as pc_spdTC_dx_sum, 
		sum(rx_clm_dx) as rx_clm_dx_sum, 
		sum(rx_qty_dx) as rx_qty_dx_sum, 
		sum(rx_spd_dx) as rx_spd_dx_sum, 
		sum(rx_spdTC_dx) as rx_spdTC_dx_sum
	from pop_for_stats_msas_spdTC
	group by year, age_cell, age_cat, st_msa,dis_cat, dual_cat, foster_cat, ltss_cat, mc_cat, calculated cbsatitle_fx;
quit;

/*put overall stats into final table*/
proc sql;
	create table unique_msas as
	select distinct st_msa
	from ahrf_msa_xwalk;

	create table st_msa_collapsed (drop=st_msa rename=(new_st_msa=st_msa)) as
	select pop.*,  
	pctl.*,
	msa.*,
	coalesce(pop.st_msa, msa.st_msa) as new_st_msa
	from pop_for_stats_msalevel  pop
		full join pctls_st_msa pctl on pop.st_msa=pctl.st_msa
		full join unique_msas msa on pop.st_msa=msa.st_msa
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
    create table space.temp_dia2l as
    select msa.*, 
        wag.WORK_fx as wageind_WORK, wag.PE_fx as wageind_PE, wag.MPE_fx as wageind_MPE
    from st_msa_collapsed msa left join wageindex_2012_impu wag
    on msa.st_msa = wag.clms_st_msa;
quit;

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
