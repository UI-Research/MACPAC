/*******************************************************************************************************************/ 
/*	Purpose: Aggregate cleaned up MAX data from 01_studypop_analyticfile with CDPS data and AHRF and MSA data 
/*			on user-input geographic variable			
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)
/*	Notes: 
/*		1) Collapse macros for easier manipulation
/*******************************************************************************************************************/ 

/* Macro vars to change*/
%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.));
%let space_name = %sysfunc(date(),date9.);
%let indata_max = space.id_pop_07dec2018;

/*Options to change*/
options obs=500000;
*proc printto print="P:\MCD-SPVR\log\02_max_ahrf_&sysdate..lst"
               log="P:\MCD-SPVR\log\02_max_ahrf_&sysdate..log" NEW;
*run;

/*Libraries*/
libname data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;

/********************************************************/
/*Initial processing to attach MSA and HRR info to files*/
/********************************************************/


%macro msa;
	data ahrf_msa_xwalk;
		set space.ahrf_msa;
		year=2012;
		if state_fips in('72') then state_cd='PR';
		if metropolitanmicropolitanstatis in('Metropolitan Statistical Area') then 
		   st_msa = catx('-', state_cd, cbsacode);
		else do; 
			st_msa = catx('-', state_cd, "XXXXX");
			cbsatitle="Non-Metro-Rest-of-State";
			end;
	label st_msa="State-MSA Code";
	run;

	proc sql;
		create table ahrf_msa_xwalk as
		select *, 2012 as year, 
			case
				when state_fips = '72' then 'PR'
				else state_cd
			end as state_cd_fx,
			case 
				when metropolitanmicropolitanstatis = 'Metropolitan Statistical Area' then catx('-', state_cd, cbsacode)
				else catx('-', state_cd, "XXXXX")
			end as st_msa,
			case 
				when metropolitanmicropolitanstatis = 'Metropolitan Statistical Area' then "Non-Metro-Rest-of-State"
				else cbsatitle
			end as cbsatitle_fx
		from space.ahrf_msa;
	quit;

	proc sql;
		create table ahrf_msa_2012 as
		select state_cd_fx as state_cd, st_msa, cbsatitle_fx as cbsatitle,
			catx('-', state_cd_fx,county_fips) as st_cnty,
			1000*sum(hos_n)/sum(pop) as beds, 
			1000*sum(md_n)/sum(pop) as md, 
			sum(poverty_d)/sum(poverty_n) as povrate,
			sum(unemp_d)/sum(unemp_n) as urate, 
			0 as _ahrf_msg
		from ahrf_msa_xwalk (drop = cbsatitle state_cd)
		group by year, state_cd_fx, st_msa, cbsatitle_fx;
	quit;

	proc sql ;
		create table max_msa_2012_all as
		select  a.*, b.st_msa, b.cbsatitle, b._ahrf_msg, b.beds, b.md, b.urate, b.povrate
		from indata_max a left join ahrf_msa_2012 b
		on a.cnty_fx=b.st_cnty;
	quit;

	proc freq data=max_msa_2012_all;
		tables st_msa cbsatitle cnty_fx/list missing;
		format st_msa cbsatitle cnty_fx $missing_char.;
	run;

	proc sql;
		title Obs without ST_MSA matches;
		select st_cnty as county,count(st_cnty) as number_missing
		from max_msa_2012_all
		where st_msa = ' '
		group by st_cnty;
	quit;

	proc sql;
		create table max_msa_2012 as
		select *
		from max_msa_2012_all
		where st_msa ne ' ';
	quit;
%mend;

%msa;

%macro hrr;
	* HRR-ZIP mapping file to merge wtih MAX data;
	PROC IMPORT OUT= hrr_zip DATAFILE="\\sas1_alt\MCD-SPVR\data\NO_PII\HRR\Dartmouth_Atlas\ZipHsaHrr12.xls"
	            DBMS=xls REPLACE;
	RUN;

	data hrr_zip;
	  set hrr_zip;
	  
	  format tmp1 $ 5. hrrnum_c $ 3.;
	  length hrrname $27.;

	  * rename zip code for merge;
	  zip_code=zipcode12;
	  tmp1 = put(hrrnum,5.);
	  tmp1=compress(tmp1);
	  len=length(tmp1);

	  * add leading zeros;
	  if len=2 then tmp1=cat('0',tmp1);
	  else if len=1 then tmp1=cat('00',tmp1);
	  tmp1=compress(tmp1);
	  hrrnum_c=tmp1;

	  * add HRR name = HRR City & HRR state;
	  hrrname=cat(trim(hrrcity),'-',trim(hrrstate));
	  len2=length(hrrname);
	  
	  keep zip_code hrrnum hrrnum_c hrrname hrrstate;
	run;

	proc print data=hrr_zip (obs=10);run;
	proc sql ;
		create table max_zip_2012 as
		select  a.*, b.*, b.zip_code as hrr_zip
		from &indata_max. a left join hrr_zip b
		on a.zip_fx=b.zip_code;
	quit;

	proc freq data=max_zip_2012;
		tables hrr_zip zip_code zip_fx/list missing;
		format hrr_zip zip_code zip_fx missing_zip.;
	run;

	proc sql;
		title Obs without HRR Zip matches;
		select zip_fx as zip_code,count(zip_fx) as number_missing
		from max_zip_2012
		where zip_code = .
		group by zip_fx;
	quit;
	title;
%mend;
%hrr;

/***************************/
/*Collapse to specified var*/
/***************************/
%macro collapse (indata,collapse_on,outdata);
	proc sql;
		create table &outdata. as
			select year, cell_age, mc_cat, dis_cat, ltss_cat, dual_cat, foster_cat, state_cd, &collapse_on.,
			sum(TOT_MDCD_PYMT_AMT) as spending,
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
			sum(age_85p)/sum(cell_n) as _85p
			from &indata.
			GROUP BY year, cell_age, mc_cat, dis_cat, ltss_cat, dual_cat, foster_cat, state_cd, &collapse_on.;
		quit;

%mend;

%collapse(indata=max_msa_2012,collapse_on=%str(st_msa,cbsatitle),outdata=msa_collapse);
%collapse(indata=max_zip_2012,collapse_on=hrrnum,outdata=hrr_collapse);

/*********************************/
/*Get statistics and add to table*/
/*********************************/
%macro get_stats(indata=, collapsevar=,outdata=);
%let indata=test;
%let collapsevar = st_msa;
	proc univariate data=&indata. noprint;
		class cell_age &collapsevar. ;
		var spending;
		output out=spend_pctls_&collapsevar.
		pctlpts = 10 25 50 75 90 95 99
		pctlpre=spd_p;
	run;

	proc univariate data=&indata. noprint;
		class  &collapsevar. ;
		var spending;
		output out=spend_cap_&collapsevar.
		pctlpts =  99.5
		pctlpre=p;
	run;

	/*put stats together*/
	proc sql;
	  create table max_msa_2012_c AS
		select a.st_msa, a.cell_age, a.spending as mcd_spd, ((a.spending>B.p99_5)*B.p99_5) as mcd_spd_TC 
		from &indata a left join spend_cap_&collapsevar. b  
		on a.st_msa=b.st_msa;
	  quit;

	  /*get overall stats*/
	proc univariate data=max_msa_2012_c noprint;
		class cell_age st_msa ;
		var mcd_spd mcd_spd_TC;
		output out=max_msaspend_2012_&collapsevar.
		sum=mcd_spd_tot mcd_spd_tot_TC 
		mean=spd_avg spd_avg_TC
		stdmean=spd_se spd_se_TC
		max=spd_max spd_TC_max
		;
	run;
	
	/*put overall stats into final table*/
	proc sql;
		create table fintab_&collapsevar._ac as
		select a.*,  
		B.mcd_spd_tot, 
		B.spd_avg, 
		B.spd_se, 
		B.spd_avg_tc, 
		B.spd_se_tc, 
		C.spd_p10, 
		C.spd_p25,
		C.spd_p50,
		C.spd_p75,
		C.spd_p90,
		C.spd_p95,
		C.spd_p99,
		B.spd_tc_max, 
		B.spd_max
		from &indata a
			left join max_msaspend_2012_&collapsevar. b on A.cell_age=B.cell_age  and a.st_msa=b.st_msa
			left join spend_pctls_&collapsevar. c on A.cell_age=C.cell_age and a.st_msa=c.st_msa
			; 
	quit;

	%let maxvars = dual_mon mc_mon dis_mon ltss_mon elg_mon cell_n d_cell_n died_n mas_: boe_: male: _0: _1_5: _6_18: _19_44: _45_64: _65_84: _85p: mcd_spd_tot spd:;

	*mark too-small cells missing;
	data &outdata. ;
		set fintab_&collapsevar._ac;
	  array all_cells {*} &maxvars.; 
	  if cell_n<11 then do;
	    do i=1 to dim(all_cells);
	      all_cells(i)=.S;
	    end;
	  end;
	  drop i;
	run;
%mend;

%get_stats(indata=msa_collapse, collapsevar=st_msa,outdata=msa_2012)
%get_stats(indata=hrr_collapse, collapsevar=hrrnum,outdata=hrr_2012)

proc sql outobs=10;
	select * from msa_2012 order by cell_n desc;

	select * from hrr_2012 order by cell_n desc;
quit;


/*

*export a stata copy;
proc export data=out.msa_allcells_&date.
  outfile= "P:\MCD-SPVR\data\workspace\output\msa_allcells_&date..dta" replace;
run;

proc export data=out.msa_nosmallcells_&date.
  outfile= "P:\MCD-SPVR\data\workspace\output\msa_nosmallcells_&date..dta" replace;
run;
