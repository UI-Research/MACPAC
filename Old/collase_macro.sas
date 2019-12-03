%macro collapse(in_data,collapse_on, orig_data);
	*3 get means of spending and CDPS and generate mult_c=(spend_c/cdps_c);
	*we are not looking at age or service categories any more so this is a mean by CDPS category;
	proc sql;
		create table spendavg as
		select svc_dx,
			AVG(TOT_MDCD_PYMT_AMT) as mspend_c,
			AVG(CDPS_SCORE) as cdps_c,
			AVG(TOT_MDCD_PYMT_AMT) / AVG(CDPS_SCORE) as mult_c
		from &in_data.
		group by svc_dx;
	quit;

	*4. join means to individual records by 'chip','nmcd','','msg'  and 
	   generate pspend_i=spend_i*mult_c and rspend_i=spend_i-pspend_i (SQL);
	proc sql;
		create table &in_data._res as
		select T1.*,
			(T1.TOT_MDCD_PYMT_AMT) AS mspend_i,
			T2.mspend_c,
			T2.cdps_c,
			T2.mult_c,
			T1.CDPS_SCORE*T2.mult_c AS Pspend_i,
			(T1.TOT_MDCD_PYMT_AMT) - T1.CDPS_SCORE*T2.mult_c AS Rspend_i
		from &in_data. (drop= mspend_i mspend_c cdps_c mult_c Pspend_i Rspend_i) T1 left join spendavg T2 /*drop those vars bc they were calculated for the entire population, not CDPS-specific*/
		on T1.svc_dx=T2.svc_dx;
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
		select  a.*, b.*
		from &in_data._res a left join ahrf_msa_xwalk (drop=year) b
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
		create table &in_data._msas as
		select *
		from max_2012_msa_join
		where st_msa ne ' ';
	quit;

	/*get top coded spending vars*/
	proc univariate data=&in_data._msas noprint;
		class &collapse_on.;
		var TOT_MDCD_PYMT_AMT Rspend_i FFS_PYMT_AMT_01 phys_clin_spending FFS_PYMT_AMT_16 MDCD_PYMT_AMT_ot11 MDCD_PYMT_AMT_lt;
		output out=spend_cap_&collapse_on.
		pctlpts = 99.5
		pctlpre=mdcd_spd_p res_spd_p inpt_spd_p physclin_spd_p rx_p otpt_spd_p nf_spd_p;
	run;

	proc sql;
	  create table &in_data._msas_spdTC AS
		select a.*, 
			case when a.TOT_MDCD_PYMT_AMT>B.mdcd_spd_p99_5 then B.mdcd_spd_p99_5 else a.TOT_MDCD_PYMT_AMT end as mdcd_spdTC,
			case when a.Rspend_i>B.res_spd_p99_5 then B.res_spd_p99_5 else a.Rspend_i end as res_spdTC,
			case when a.FFS_PYMT_AMT_01>B.inpt_spd_p99_5 then B.inpt_spd_p99_5 else a.FFS_PYMT_AMT_01 end as inpt_spdTC,
			case when a.phys_clin_spending>B.physclin_spd_p99_5 then B.physclin_spd_p99_5 else a.phys_clin_spending end as physclin_spdTC,
			case when a.FFS_PYMT_AMT_16>B.rx_p99_5 then B.rx_p99_5 else a.FFS_PYMT_AMT_16 end as rx_spdTC,
			case when a.MDCD_PYMT_AMT_ot11>B.otpt_spd_p99_5 then B.otpt_spd_p99_5 else a.MDCD_PYMT_AMT_ot11 end as otpt_spdTC,
			case when a.MDCD_PYMT_AMT_lt>B.nf_spd_p99_5 then B.nf_spd_p99_5 else a.MDCD_PYMT_AMT_lt end as nf_spdTC
		from &in_data._msas a left join spend_cap_&collapse_on. b  
		on a.&collapse_on.=b.&collapse_on.;
	  quit;

	  /*get spending percentiles*/
	proc univariate data=&in_data._msas_spdTC noprint;
		class &collapse_on. ;
		var TOT_MDCD_PYMT_AMT Rspend_i FFS_PYMT_AMT_01 phys_clin_spending FFS_PYMT_AMT_16 MDCD_PYMT_AMT_ot11 MDCD_PYMT_AMT_lt
			mdcd_spdTC res_spdTC inpt_spdTC physclin_spdTC rx_spdTC otpt_spdTC nf_spdTC;
		output out=spend_pctls_&collapse_on.
		pctlpts = 10 25 50 75 90 95 99
		pctlpre=mdcd_spd_p res_spd_p inpt_spd_p physclin_spd_p otpt_spd_p nf_spd_p
				mdcd_spdTC_p res_spdTC_p inpt_spdTC_p physclin_spdTC_p rx_spdTC_p otpt_spdTC_p nf_spdTC_p;
	run;
proc means data=spend_pctls_&collapse_on.;run;
	/*get basic summary stats for each MSA*/
	proc sql;
		create table &in_data._collapsed as
			select year, &collapse_on., 
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
			sum(TOT_MDCD_PYMT_AMT)/ count(distinct bene_id) as mdcd_spd_sum,
			min(TOT_MDCD_PYMT_AMT)/ count(distinct bene_id) as mdcd_spd_min,
			mean(TOT_MDCD_PYMT_AMT)/ count(distinct bene_id) as mdcd_spd_mean,
			max(TOT_MDCD_PYMT_AMT)/ count(distinct bene_id) as mdcd_spd_max,
			stderr(TOT_MDCD_PYMT_AMT)/ count(distinct bene_id) as mdcd_spd_stderr,

			sum(mdcd_spdTC)/ count(distinct bene_id) as mdcd_spdTC_sum,
			min(mdcd_spdTC)/ count(distinct bene_id) as mdcd_spdTC_min,
			mean(mdcd_spdTC)/ count(distinct bene_id) as mdcd_spdTC_mean,
			max(mdcd_spdTC)/ count(distinct bene_id) as mdcd_spdTC_max,
			stderr(mdcd_spdTC)/ count(distinct bene_id) as mdcd_spdTC_stderr,

			sum(FFS_PYMT_AMT_01)/ count(distinct bene_id) as inpt_spd_sum,
			min(FFS_PYMT_AMT_01)/ count(distinct bene_id) as inpt_spd_min,
			mean(FFS_PYMT_AMT_01)/ count(distinct bene_id) as inpt_spd_mean,
			max(FFS_PYMT_AMT_01)/ count(distinct bene_id) as inpt_spd_max,
			stderr(FFS_PYMT_AMT_01)/ count(distinct bene_id) as inpt_spd_stderr,

			sum(inpt_spdTC)/ count(distinct bene_id) as inpt_spdTC_sum,
			min(inpt_spdTC)/ count(distinct bene_id) as inpt_spdTC_min,
			mean(inpt_spdTC)/ count(distinct bene_id) as inpt_spdTC_mean,
			max(inpt_spdTC)/ count(distinct bene_id) as inpt_spdTC_max,
			stderr(inpt_spdTC)/ count(distinct bene_id) as inpt_spdTC_stderr,

			sum(phys_clin_spending)/ count(distinct bene_id) as physclin_spd_sum, 
			min(phys_clin_spending)/ count(distinct bene_id) as physclin_spd_min, 
			mean(phys_clin_spending)/ count(distinct bene_id) as physclin_spd_mean, 
			max(phys_clin_spending)/ count(distinct bene_id) as physclin_spd_max, 
			stderr(phys_clin_spending)/ count(distinct bene_id) as physclin_spd_stderr,

			sum(physclin_spdTC)/ count(distinct bene_id) as physclin_spdTC_sum, 
			min(physclin_spdTC)/ count(distinct bene_id) as physclin_spdTC_min, 
			mean(physclin_spdTC)/ count(distinct bene_id) as physclin_spdTC_mean, 
			max(physclin_spdTC)/ count(distinct bene_id) as physclin_spdTC_max, 
			stderr(physclin_spdTC)/ count(distinct bene_id) as physclin_spdTC_stderr,

			sum(FFS_PYMT_AMT_16)/ count(distinct bene_id) as rx_spd_sum,
			min(FFS_PYMT_AMT_16)/ count(distinct bene_id) as rx_spd_min,
			mean(FFS_PYMT_AMT_16)/ count(distinct bene_id) as rx_spd_mean,
			max(FFS_PYMT_AMT_16)/ count(distinct bene_id) as rx_spd_max,
			stderr(FFS_PYMT_AMT_16)/ count(distinct bene_id) as rx_spd_stderr,
			
			sum(rx_spdTC)/ count(distinct bene_id) as rx_spdTC_sum,
			min(rx_spdTC)/ count(distinct bene_id) as rx_spdTC_min,
			mean(rx_spdTC)/ count(distinct bene_id) as rx_spdTC_mean,
			max(rx_spdTC)/ count(distinct bene_id) as rx_spdTC_max,
			stderr(rx_spdTC)/ count(distinct bene_id) as rx_spdTC_stderr,

			sum(MDCD_PYMT_AMT_ot11)/ count(distinct bene_id) as otpt_spd_sum,
			min(MDCD_PYMT_AMT_ot11)/ count(distinct bene_id) as otpt_spd_min,
			mean(MDCD_PYMT_AMT_ot11)/ count(distinct bene_id) as otpt_spd_mean,
			max(MDCD_PYMT_AMT_ot11)/ count(distinct bene_id) as otpt_spd_max,
			stderr(MDCD_PYMT_AMT_ot11)/ count(distinct bene_id) as otpt_spd_stderr,

			sum(otpt_spdTC)/ count(distinct bene_id) as otpt_spdTC_sum,
			min(otpt_spdTC)/ count(distinct bene_id) as otpt_spdTC_min,
			mean(otpt_spdTC)/ count(distinct bene_id) as otpt_spdTC_mean,
			max(otpt_spdTC)/ count(distinct bene_id) as otpt_spdTC_max,
			stderr(otpt_spdTC)/ count(distinct bene_id) as otpt_spdTC_stderr,

			sum(MDCD_PYMT_AMT_lt)/ count(distinct bene_id) as nf_spd_sum,
			min(MDCD_PYMT_AMT_lt)/ count(distinct bene_id) as nf_spd_min,
			mean(MDCD_PYMT_AMT_lt)/ count(distinct bene_id) as nf_spd_mean,
			max(MDCD_PYMT_AMT_lt)/ count(distinct bene_id) as nf_spd_max,
			stderr(MDCD_PYMT_AMT_lt)/ count(distinct bene_id) as nf_spd_stderr,

			sum(nf_spdTC)/ count(distinct bene_id) as nf_spdTC_sum,
			min(nf_spdTC)/ count(distinct bene_id) as nf_spdTC_min,
			mean(nf_spdTC)/ count(distinct bene_id) as nf_spdTC_mean,
			max(nf_spdTC)/ count(distinct bene_id) as nf_spdTC_max,
			stderr(nf_spdTC)/ count(distinct bene_id) as nf_spdTC_stderr,

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
			sum(TOT_IP_STAY_CNT)/ count(distinct bene_id) as inpt_clm_sum,
			min(TOT_IP_STAY_CNT)/ count(distinct bene_id) as inpt_clm_min,
			mean(TOT_IP_STAY_CNT)/ count(distinct bene_id) as inpt_clm_mean,
			max(TOT_IP_STAY_CNT)/ count(distinct bene_id) as inpt_clm_max,
			stderr(TOT_IP_STAY_CNT)/ count(distinct bene_id) as inpt_clm_stderr,

			sum(phys_clin_claims)/ count(distinct bene_id) as physclin_clm_sum,
			min(phys_clin_claims)/ count(distinct bene_id) as physclin_clm_min,
			mean(phys_clin_claims)/ count(distinct bene_id) as physclin_clm_mean,
			max(phys_clin_claims)/ count(distinct bene_id) as physclin_clm_max,
			stderr(phys_clin_claims)/ count(distinct bene_id) as physclin_clm_stderr,

			sum(FFS_CLM_CNT_16)/ count(distinct bene_id) as rx_clm_sum,
			min(FFS_CLM_CNT_16)/ count(distinct bene_id) as rx_clm_min,
			mean(FFS_CLM_CNT_16)/ count(distinct bene_id) as rx_clm_mean,
			max(FFS_CLM_CNT_16)/ count(distinct bene_id) as rx_clm_max,
			stderr(FFS_CLM_CNT_16)/ count(distinct bene_id) as rx_clm_stderr,

			sum(MSIS_TOS_ot11)/ count(distinct bene_id) as otpt_clm_sum,
			min(MSIS_TOS_ot11)/ count(distinct bene_id) as otpt_clm_min,
			mean(MSIS_TOS_ot11)/ count(distinct bene_id) as otpt_clm_mean,
			max(MSIS_TOS_ot11)/ count(distinct bene_id) as otpt_clm_max,
			stderr(MSIS_TOS_ot11)/ count(distinct bene_id) as otpt_clm_stderr,

			sum(lt_MSIS_TOS)/ count(distinct bene_id) as nf_clm_sum,
			min(lt_MSIS_TOS)/ count(distinct bene_id) as nf_clm_min,
			mean(lt_MSIS_TOS)/ count(distinct bene_id) as nf_clm_mean,
			max(lt_MSIS_TOS)/ count(distinct bene_id) as nf_clm_max,
			stderr(lt_MSIS_TOS)/ count(distinct bene_id) as nf_clm_stderr
			from &in_data._msas_spdTC
			group by year, &collapse_on.;
		quit;

	proc sql;
		/*join the beds, md, urate, and povrate to msa data*/
		create table arhf_vars_added as 
		select a.*, b.beds, b.md, b.urate, b.povrate
		from &in_data._collapsed as a left join 
			(select distinct &collapse_on., beds, md, urate, povrate from &orig_data.) as b
		on a.&collapse_on.=b.&collapse_on.;
	quit;

	proc univariate data=&in_data._msas noprint;
		class &collapse_on. ;
		var TOT_IP_STAY_CNT phys_clin_claims FFS_CLM_CNT_16 MSIS_TOS_ot11 lt_MSIS_TOS;
		output out=count_pctls_&collapse_on.
		pctlpts = 10 25 50 75 90 95 99
		pctlpre=inpt_clm_p physclin_clm_p rx_clm_p otpt_clm_p nf_clm_p;
	run;
	/*put overall stats into final table*/
	proc sql;
		create table unique_msas as
		select distinct st_msa
		from ahrf_msa_xwalk;

		create table fintab_&collapse_on._ac (drop=st_msa rename=(new_st_msa=st_msa)) as
		select a.*,  
		b.*,
		c.*,
		d.*,
		coalesce(a.st_msa, d.st_msa) as new_st_msa
		from arhf_vars_added a 
			full join spend_pctls_&collapse_on. b on a.&collapse_on.=b.&collapse_on.
			full join count_pctls_&collapse_on. c on a.&collapse_on.=c.&collapse_on.
			full join unique_msas d on a.&collapse_on.=d.&collapse_on.
		where d.&collapse_on. ne '';
	quit;

	%let maxvars = dual_mon mc_mon dis_mon ltss_mon elg_mon cell_n d_servicetype_n died_n mas_: boe_: male: _0: _1_5: _6_18: _19_44: _45_64: _65_84: _85p: cdps no_cdps_condsf spd: res:;

	*mark too-small cells missing;
	data space.&in_data._collapsed ;
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
			no_cdps_conds="Proportion of beneficiaries with no CDPS diagnoses in year"s
			beds ="Number of hospital beds per 1k people, 2010"
			md ="Number of physicians per 1k people, 2010"
			urate ="Unemployment rate, 2012"
			povrate ="Rate of persons in poverty, 2012"

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

			inpt_spdTC_sum ="Sum Inpatient Spending Per Beneficiary (top coded)"
			inpt_spdTC_min ="Minimum Inpatient Spending Per Beneficiary (top coded)"
			inpt_spdTC_mean ="Mean Inpatient Spending Per Beneficiary (top coded)"
			inpt_spdTC_max ="Maximum Inpatient Spending Per Beneficiary (top coded)"
			inpt_spdTC_stderr ="Standard Error of Mean Inpatient Spending Per Beneficiary (top coded)"
			inpt_spd_sum ="Sum Inpatient Spending Per Beneficiary"
			inpt_spd_min ="Minimum Inpatient Spending Per Beneficiary"
			inpt_spd_mean ="Mean Inpatient Spending Per Beneficiary"
			inpt_spd_max ="Maximum Inpatient Spending Per Beneficiary"
			inpt_spd_stderr ="Standard Error of Mean Inpatient Spending"
			inpt_spd_p10 ="10th Percentile of Inpatient Spending"
			inpt_spd_p25 ="25th Percentile of Inpatient Spending"
			inpt_spd_p50 ="50th Percentile of Inpatient Spending"
			inpt_spd_p75 ="75th Percentile of Inpatient Spending"
			inpt_spd_p90 ="90th Percentile of Inpatient Spending"
			inpt_spd_p95 ="95th Percentile of Inpatient Spending"
			inpt_spd_p99 ="99th Percentile of Inpatient Spending"

			physclin_spdTC_sum ="Sum Physician and Clinic Spending Per Beneficiary (top coded)"
			physclin_spdTC_min ="Minimum Physician and Clinic Spending Per Beneficiary (top coded)"
			physclin_spdTC_mean ="Mean Physician and Clinic Spending Per Beneficiary (top coded)"
			physclin_spdTC_max ="Maximum Physician and Clinic Spending Per Beneficiary (top coded)"
			physclin_spdTC_stderr ="Standard Error of Mean Physician and Clinic Spending Per Beneficiary (top coded)"
			physclin_spd_sum ="Sum Physician and Clinic Spending Per Beneficiary"
			physclin_spd_min ="Minimum Physician and Clinic Spending Per Beneficiary"
			physclin_spd_mean ="Mean Physician and Clinic Spending Per Beneficiary"
			physclin_spd_max ="Maximum Physician and Clinic Spending Per Beneficiary"
			physclin_spd_stderr ="Standard Error of Mean Physician and Clinic Spending"
			physclin_spd_p10 ="10th Percentile of Physician and Clinic Spending"
			physclin_spd_p25 ="25th Percentile of Physician and Clinic Spending"
			physclin_spd_p50 ="50th Percentile of Physician and Clinic Spending"
			physclin_spd_p75 ="75th Percentile of Physician and Clinic Spending"
			physclin_spd_p90 ="90th Percentile of Physician and Clinic Spending"
			physclin_spd_p95 ="95th Percentile of Physician and Clinic Spending"
			physclin_spd_p99 ="99th Percentile of Physician and Clinic Spending"

			rx_spdTC_sum ="Sum Prescription Drug Spending Per Beneficiary (top coded)"
			rx_spdTC_min ="Minimum Prescription Drug Spending Per Beneficiary (top coded)"
			rx_spdTC_mean ="Mean Prescription Drug Spending Per Beneficiary (top coded)"
			rx_spdTC_max ="Maximum Prescription Drug Spending Per Beneficiary (top coded)"
			rx_spdTC_stderr ="Standard Error of Mean Prescription Drug Spending Per Beneficiary (top coded)"
			rx_spd_sum ="Sum Prescription Drug Spending Per Beneficiary"
			rx_spd_min ="Minimum Prescription Drug Spending Per Beneficiary"
			rx_spd_mean ="Mean Prescription Drug Spending Per Beneficiary"
			rx_spd_max ="Maximum Prescription Drug Spending Per Beneficiary"
			rx_spd_stderr ="Standard Error of Mean Prescription Drug Spending"
			rx_spd_p10 ="10th Percentile of Prescription Drug Spending"
			rx_spd_p25 ="25th Percentile of Prescription Drug Spending"
			rx_spd_p50 ="50th Percentile of Prescription Drug Spending"
			rx_spd_p75 ="75th Percentile of Prescription Drug Spending"
			rx_spd_p90 ="90th Percentile of Prescription Drug Spending"
			rx_spd_p95 ="95th Percentile of Prescription Drug Spending"
			rx_spd_p99 ="99th Percentile of Prescription Drug Spending"

			nf_spdTC_sum ="Sum Nursing Facility Spending Per Beneficiary (top coded)"
			nf_spdTC_min ="Minimum Nursing Facility Spending Per Beneficiary (top coded)"
			nf_spdTC_mean ="Mean Nursing Facility Spending Per Beneficiary (top coded)"
			nf_spdTC_max ="Maximum Nursing Facility Spending Per Beneficiary (top coded)"
			nf_spdTC_stderr ="Standard Error of Mean Nursing Facility Spending Per Beneficiary (top coded)"
			nf_spd_sum ="Sum Nursing Facility Spending Per Beneficiary"
			nf_spd_min ="Minimum Nursing Facility Spending Per Beneficiary"
			nf_spd_mean ="Mean Nursing Facility Spending Per Beneficiary"
			nf_spd_max ="Maximum Nursing Facility Spending Per Beneficiary"
			nf_spd_stderr ="Standard Error of Mean Nursing Facility Spending"
			nf_spd_p10 ="10th Percentile of Nursing Facility Spending"
			nf_spd_p25 ="25th Percentile of Nursing Facility Spending"
			nf_spd_p50 ="50th Percentile of Nursing Facility Spending"
			nf_spd_p75 ="75th Percentile of Nursing Facility Spending"
			nf_spd_p90 ="90th Percentile of Nursing Facility Spending"
			nf_spd_p95 ="95th Percentile of Nursing Facility Spending"
			nf_spd_p99 ="99th Percentile of Nursing Facility Spending"

			otpt_spdTC_sum ="Sum Outpatient Hospital Spending Per Beneficiary (top coded)"
			otpt_spdTC_min ="Minimum Outpatient Hospital Spending Per Beneficiary (top coded)"
			otpt_spdTC_mean ="Mean Outpatient Hospital Spending Per Beneficiary (top coded)"
			otpt_spdTC_max ="Maximum Outpatient Hospital Spending Per Beneficiary (top coded)"
			otpt_spdTC_stderr ="Standard Error of Mean Outpatient Hospital Spending Per Beneficiary (top coded)"
			otpt_spd_sum ="Sum Outpatient Hospital Spending Per Beneficiary"
			otpt_spd_min ="Minimum Outpatient Hospital Spending Per Beneficiary"
			otpt_spd_mean ="Mean Outpatient Hospital Spending Per Beneficiary"
			otpt_spd_max ="Maximum Outpatient Hospital Spending Per Beneficiary"
			otpt_spd_stderr ="Standard Error of Mean Outpatient Hospital Spending"
			otpt_spd_p10 ="10th Percentile of Outpatient Hospital Spending"
			otpt_spd_p25 ="25th Percentile of Outpatient Hospital Spending"
			otpt_spd_p50 ="50th Percentile of Outpatient Hospital Spending"
			otpt_spd_p75 ="75th Percentile of Outpatient Hospital Spending"
			otpt_spd_p90 ="90th Percentile of Outpatient Hospital Spending"
			otpt_spd_p95 ="95th Percentile of Outpatient Hospital Spending"
			otpt_spd_p99 ="99th Percentile of Outpatient Hospital Spending" 

			inpt_clm_sum ="Sum Inpatient Claims Per Beneficiary"
			inpt_clm_min ="Minimum Inpatient Claims Per Beneficiary"
			inpt_clm_mean ="Mean Inpatient Claims Per Beneficiary"
			inpt_clm_max ="Maximum Inpatient Claims Per Beneficiary"
			inpt_clm_stderr ="Standard Error of Mean Inpatient Claims"
			inpt_clm_p10 ="10th Percentile of Inpatient Claims"
			inpt_clm_p25 ="25th Percentile of Inpatient Claims"
			inpt_clm_p50 ="50th Percentile of Inpatient Claims"
			inpt_clm_p75 ="75th Percentile of Inpatient Claims"
			inpt_clm_p90 ="90th Percentile of Inpatient Claims"
			inpt_clm_p95 ="95th Percentile of Inpatient Claims"
			inpt_clm_p99 ="99th Percentile of Inpatient Claims" 

			physclin_clm_sum ="Sum Physician and Clinic Claims Per Beneficiary"
			physclin_clm_min ="Minimum Physician and Clinic Claims Per Beneficiary"
			physclin_clm_mean ="Mean Physician and Clinic Claims Per Beneficiary"
			physclin_clm_max ="Maximum Physician and Clinic Claims Per Beneficiary"
			physclin_clm_stderr ="Standard Error of Mean Physician and Clinic Claims"
			physclin_clm_p10 ="10th Percentile of Physician and Clinic Claims"
			physclin_clm_p25 ="25th Percentile of Physician and Clinic Claims"
			physclin_clm_p50 ="50th Percentile of Physician and Clinic Claims"
			physclin_clm_p75 ="75th Percentile of Physician and Clinic Claims"
			physclin_clm_p90 ="90th Percentile of Physician and Clinic Claims"
			physclin_clm_p95 ="95th Percentile of Physician and Clinic Claims"
			physclin_clm_p99 ="99th Percentile of Physician and Clinic Claims" 

			rx_clm_sum ="Sum Prescription Drug Claims Per Beneficiary"
			rx_clm_min ="Minimum Prescription Drug Claims Per Beneficiary"
			rx_clm_mean ="Mean Prescription Drug Claims Per Beneficiary"
			rx_clm_max ="Maximum Prescription Drug Claims Per Beneficiary"
			rx_clm_stderr ="Standard Error of Mean Prescription Drug Claims"
			rx_clm_p10 ="10th Percentile of Prescription Drug Claims"
			rx_clm_p25 ="25th Percentile of Prescription Drug Claims"
			rx_clm_p50 ="50th Percentile of Prescription Drug Claims"
			rx_clm_p75 ="75th Percentile of Prescription Drug Claims"
			rx_clm_p90 ="90th Percentile of Prescription Drug Claims"
			rx_clm_p95 ="95th Percentile of Prescription Drug Claims"
			rx_clm_p99 ="99th Percentile of Prescription Drug Claims" 

			otpt_clm_sum ="Sum Outpatient Hospital Claims Per Beneficiary"
			otpt_clm_min ="Minimum Outpatient Hospital Claims Per Beneficiary"
			otpt_clm_mean ="Mean Outpatient Hospital Claims Per Beneficiary"
			otpt_clm_max ="Maximum Outpatient Hospital Claims Per Beneficiary"
			otpt_clm_stderr ="Standard Error of Mean Outpatient Hospital Claims"
			otpt_clm_p10 ="10th Percentile of Outpatient Hospital Claims"
			otpt_clm_p25 ="25th Percentile of Outpatient Hospital Claims"
			otpt_clm_p50 ="50th Percentile of Outpatient Hospital Claims"
			otpt_clm_p75 ="75th Percentile of Outpatient Hospital Claims"
			otpt_clm_p90 ="90th Percentile of Outpatient Hospital Claims"
			otpt_clm_p95 ="95th Percentile of Outpatient Hospital Claims"
			otpt_clm_p99 ="99th Percentile of Outpatient Hospital Claims" 

			nf_clm_sum ="Sum Nursing Facility Claims Per Beneficiary"
			nf_clm_min ="Minimum Nursing Facility Claims Per Beneficiary"
			nf_clm_mean ="Mean Nursing Facility Claims Per Beneficiary"
			nf_clm_max ="Maximum Nursing Facility Claims Per Beneficiary"
			nf_clm_stderr ="Standard Error of Mean Nursing Facility Claims"
			nf_clm_p10 ="10th Percentile of Nursing Facility Claims"
			nf_clm_p25 ="25th Percentile of Nursing Facility Claims"
			nf_clm_p50 ="50th Percentile of Nursing Facility Claims"
			nf_clm_p75 ="75th Percentile of Nursing Facility Claims"
			nf_clm_p90 ="90th Percentile of Nursing Facility Claims"
			nf_clm_p95 ="95th Percentile of Nursing Facility Claims"
			nf_clm_p99 ="99th Percentile of Nursing Facility Claims" 
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

proc means data=space.dia2l_cdps_benes_collapsed;run;

