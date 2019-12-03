/*******************************************************************************************************************/ 
/*	Purpose: Using a yearly MAX input data set, create an analytic file with specified variables. 
/*				Fix invalid geographic information.
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)
/*	Notes: 
/*		Drop benes with partial Medicaid eligibility when cell type is determined (last observed month). Add a summary variable on the number of total partial benefit months in the cell.
/*		Include small cell size for internal purposes
/*		Include top-coded spending summary series (sum, mean, etc.)
/*		1) Collapse macros for easier manipulation
/*		2) Dependent on MAX data formatting
/*******************************************************************************************************************/ 
/*Options to change*/
%macro prod();
	options obs=MAX;
	/*Log*/
	proc printto print="P:\MCD-SPVR\log\01_studypop_fx_&sysdate..lst"
	               log="P:\MCD-SPVR\log\01_studypop_fx_&sysdate..log" NEW;
	run;
%mend;

%macro test();	
	options obs=100000;
	/*Log*/
	proc printto;run;
%mend;

%prod();
*%test();

* Macro variables for processing;
%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.)); /*date+time for file names, to ensure files aren't overwritten*/
%let space_name = %sysfunc(date(),date9.); /*date for naming convention in space lib*/
%let dum_tab = %str(ods excel options(sheet_interval="table"); ods exclude all; data _null_; file print; put _all_; run; ods select all;); /*adds a dummy table to make a new worksheet ods excel*/
%let report_folder = P:\MCD-SPVR\reports; /*location of output reports*/

/*macros to change*/
%let indata_year = 2012; /*claims year*/
%let age_date = 12,01,2012; /*date from which to calculate age. must be month,day,year*/

/*libraries*/
libname data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;


proc format library=library;
	value num_notzero
		. = 'Missing'
		0 = 'Zero'
		other = 'Not zero';
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
	value $elig_label
		  '00' = "NOT ELIGIBLE"
		  '11' = "AGED, CASH"
		  '12' = "BLIND/DISABLED, CASH"
		  '14' = "CHILD (NOT CHILD OF UNEMPLOYED ADULT, NOT FOSTER CARE CHILD)"
		  '15' = "ADULT (NOT BASED ON UNEMPLOYMENT STATUS)"
		  '16' = "CHILD OF UNEMPLOYED ADULT"
		  '17' = "UNEMPLOYED ADULT"
	      '21' = "AGED, MN" 
		  '22' = "BLIND/DISABLED, MN"
		  '24' = "CHILD, MN (FORMERLY AFDC CHILD, MN)"
		  '25' = "ADULT, MN (FORMERLY AFDC ADULT, MN)"
		  '31' = "AGED, POVERTY"
		  '32' = "BLIND/DISABLED, POVERTY"
		  '34' = "CHILD, POVERTY (INCLUDES MEDICAID EXPANSION SCHIP CHILDREN)"
		  '35' = "ADULT, POVERTY"
		  '3A' = "INDIVIDUAL COVERED UNDER THE BREAST AND CERVICAL CANCER PREVENTION ACT OF 2000, POVERTY"
		  '41' = "OTHER AGED"
		  '42' = "OTHER BLIND/DISABLED"
	 	  '44' = "OTHER CHILD"
		  '45' = "OTHER ADULT"
		  '48' = "FOSTER CARE CHILD"
		  '51' = "AGED, SECTION 1115 DEMONSTRATION EXPANSION"
		  '52' = "DISABLED, SECTION 1115 DEMONSTRATION EXPANSION"
		  '54' = "CHILD, SECTION 1115 DEMONSTRATION EXPANSION"
		  '55' = "ADULT, SECTION 1115 DEMONSTRATION EXPANSION"
		  '99' = "UNKNOWN ELIGIBILITY";
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
	value $ missfmt ' '="Missing"
			other="Not Missing";
	value nmissfmt . ="Missing"
			other="Not Missing";
quit;


%macro make_popfile(indata,outdata);
/*%let indata=data.maxdata_ps_2012; 
%let outdata=personlevel;*/
/*limit incoming variables and drop those observations missing all geo data*/

	data limit_incoming_dat;
		set &indata. (keep=BENE_ID STATE_CD EL_RSDNC_ZIP_CD_LTST EL_RSDNC_CNTY_CD_LTST EL_DOB MC_COMBO_MO:
			EL_MDCR_DUAL_MO_: EL_RSTRCT_BNFT_FLG: MAX_ELG_CD_MO: EL_CHIP_FLAG: 
			EL_SEX_CD TOT_MDCD_PYMT_AMT EL_ELGBLTY_MO_CNT EL_MAX_ELGBLTY_CD_LTST
			EL_DOD MDCR_DOD CLTC: FEE_FOR_SRVC_IND_07 TOT_MDCD_PYMT_AMT TOT_IP_STAY_CNT TOT_IP_DAY_CNT_STAYS FFS_PYMT_AMT_01
			FFS_CLM_CNT_08 FFS_CLM_CNT_12 FFS_PYMT_AMT_08 FFS_PYMT_AMT_12 FFS_CLM_CNT_16 FFS_PYMT_AMT_16);
		if missing(BENE_ID) then delete;
		if missing(EL_RSDNC_CNTY_CD_LTST) and EL_RSDNC_ZIP_CD_LTST = 00000 then delete;
	run;

	/*put observations missing geo data in a separate table for later inspection*/
	proc sql;
		create table space.unformatted_dropped_&space_name. as
		select a.*
		from &indata. (keep=BENE_ID STATE_CD EL_RSDNC_ZIP_CD_LTST EL_RSDNC_CNTY_CD_LTST EL_DOB MC_COMBO_MO:
			EL_MDCR_DUAL_MO_: EL_RSTRCT_BNFT_FLG: MAX_ELG_CD_MO: EL_CHIP_FLAG: 
			EL_SEX_CD TOT_MDCD_PYMT_AMT EL_ELGBLTY_MO_CNT EL_MAX_ELGBLTY_CD_LTST
			EL_DOD MDCR_DOD CLTC: FEE_FOR_SRVC_IND_07 TOT_MDCD_PYMT_AMT TOT_IP_STAY_CNT TOT_IP_DAY_CNT_STAYS FFS_PYMT_AMT_01
			FFS_CLM_CNT_08 FFS_CLM_CNT_12 FFS_PYMT_AMT_08 FFS_PYMT_AMT_12 FFS_CLM_CNT_16 FFS_PYMT_AMT_16) a 
			left join limit_incoming_dat b
		on a.BENE_ID = b.BENE_ID
		where b.BENE_ID is missing;
	quit;
	
	proc freq data=space.unformatted_dropped_&space_name.;
		title "Missing BENE_ID and Geo Info Unformatted Dropped File";
		tables BENE_ID*EL_RSDNC_CNTY_CD_LTST*EL_RSDNC_ZIP_CD_LTST/list missing;
		format BENE_ID EL_RSDNC_CNTY_CD_LTST $missing_char. EL_RSDNC_ZIP_CD_LTST missing_zip.;
	run;

	data temp1;
		format cell_type1-cell_type4 $16. cell $15. county_miss $3. zip_miss 5. cell_age $16. calc_date mmddyy8. full_benf_ltst $1. full_benf 2.;
		set limit_incoming_dat;

		/* get last observed month's benefit restriction flag*/
		full_benf_ltst = coalescec(EL_RSTRCT_BNFT_FLG_12, EL_RSTRCT_BNFT_FLG_11, EL_RSTRCT_BNFT_FLG_10, EL_RSTRCT_BNFT_FLG_9, EL_RSTRCT_BNFT_FLG_8, EL_RSTRCT_BNFT_FLG_7, EL_RSTRCT_BNFT_FLG_6, EL_RSTRCT_BNFT_FLG_5, 
		EL_RSTRCT_BNFT_FLG_4, EL_RSTRCT_BNFT_FLG_3, EL_RSTRCT_BNFT_FLG_2, EL_RSTRCT_BNFT_FLG_1);
		
		/*flag benes with restricted or no benefits in the last observed month*/
		if full_benf_ltst not in ("0","1") then full_benf = 99; /*partial*/
		if full_benf_ltst = "1" then full_benf = 1; /*full*/
		if full_benf_ltst = "0" then full_benf = 0; /*no benefits*/
		if full_benf_ltst = "" then full_benf = .; 
		
		/*count how many benes had partial benefits in every month*/
		if EL_RSTRCT_BNFT_FLG_12 not in ("0","1") then partial_benefits_12 = 1;
		if EL_RSTRCT_BNFT_FLG_12 = "0" then partial_benefits_12 = 99;
		if EL_RSTRCT_BNFT_FLG_12 = "1" then partial_benefits_12 = 0;
		if EL_RSTRCT_BNFT_FLG_11 not in ("0","1") then partial_benefits_11 = 1;
		if EL_RSTRCT_BNFT_FLG_11 = "0" then partial_benefits_11 = 99;
		if EL_RSTRCT_BNFT_FLG_11 = "1" then partial_benefits_11 = 0;
		if EL_RSTRCT_BNFT_FLG_10 not in ("0","1") then partial_benefits_10 = 1;
		if EL_RSTRCT_BNFT_FLG_10 = "0" then partial_benefits_10 = 99;
		if EL_RSTRCT_BNFT_FLG_10 = "1" then partial_benefits_10 = 0;
		if EL_RSTRCT_BNFT_FLG_9 not in ("0","1") then partial_benefits_9 = 1;
		if EL_RSTRCT_BNFT_FLG_9 = "0" then partial_benefits_9 = 99;
		if EL_RSTRCT_BNFT_FLG_9 = "1" then partial_benefits_9 = 0;
		if EL_RSTRCT_BNFT_FLG_8 not in ("0","1") then partial_benefits_8 = 1;
		if EL_RSTRCT_BNFT_FLG_8 = "0" then partial_benefits_8 = 99;
		if EL_RSTRCT_BNFT_FLG_8 = "1" then partial_benefits_8 = 0;
		if EL_RSTRCT_BNFT_FLG_7 not in ("0","1") then partial_benefits_7 = 1;
		if EL_RSTRCT_BNFT_FLG_7 = "0" then partial_benefits_7 = 99;
		if EL_RSTRCT_BNFT_FLG_7 = "1" then partial_benefits_7 = 0;
		if EL_RSTRCT_BNFT_FLG_6 not in ("0","1") then partial_benefits_6 = 1;
		if EL_RSTRCT_BNFT_FLG_6 = "0" then partial_benefits_6 = 99;
		if EL_RSTRCT_BNFT_FLG_6 = "1" then partial_benefits_6 = 0;
		if EL_RSTRCT_BNFT_FLG_5 not in ("0","1") then partial_benefits_5 = 1;
		if EL_RSTRCT_BNFT_FLG_5 = "0" then partial_benefits_5 = 99;
		if EL_RSTRCT_BNFT_FLG_5 = "1" then partial_benefits_5 = 0;
		if EL_RSTRCT_BNFT_FLG_4 not in ("0","1") then partial_benefits_4 = 1;
		if EL_RSTRCT_BNFT_FLG_4 = "0" then partial_benefits_4 = 99;
		if EL_RSTRCT_BNFT_FLG_4 = "1" then partial_benefits_4 = 0;
		if EL_RSTRCT_BNFT_FLG_3 not in ("0","1") then partial_benefits_3 = 1;
		if EL_RSTRCT_BNFT_FLG_3 = "0" then partial_benefits_3 = 99;
		if EL_RSTRCT_BNFT_FLG_3 = "1" then partial_benefits_3 = 0;
		if EL_RSTRCT_BNFT_FLG_2 not in ("0","1") then partial_benefits_2 = 1;
		if EL_RSTRCT_BNFT_FLG_2 = "0" then partial_benefits_2 = 99;
		if EL_RSTRCT_BNFT_FLG_2 = "1" then partial_benefits_2 = 0;
		if EL_RSTRCT_BNFT_FLG_1 not in ("0","1") then partial_benefits_1 = 1;
		if EL_RSTRCT_BNFT_FLG_1 = "0" then partial_benefits_1 = 99;
		if EL_RSTRCT_BNFT_FLG_1 = "1" then partial_benefits_1 = 0;

	  	/***********************************/
		/*Simple variable creation/recoding*/
		/***********************************/
		year = &indata_year.;
		cell_n = 1;
		pm_n = EL_ELGBLTY_MO_CNT;
		tot_pay = TOT_MDCD_PYMT_AMT;     
		recipno=catx ('_',STATE_CD,BENE_ID); /*to link the CDPS data*/
		physclin_clm_cnt = FFS_CLM_CNT_08 + FFS_CLM_CNT_12; /*physician and clinic claim count*/
		physclin_pymt_amt = FFS_PYMT_AMT_08 + FFS_PYMT_AMT_12; /*physician and clinic spending*/

		/* LTSS */
		cltc = sum(of CLTC_FFS_PYMT_AMT_11--CLTC_FFS_PYMT_AMT_40);

		if Fee_for_srvc_ind_07=1 then lt_nurs=1;
		else lt_nurs=0;
		if missing(Fee_for_srvc_ind_07) then _ltss=1;

		if cltc>0 or lt_nurs=1 then ltss=1;
		else ltss=0;

		/* calculate age */
		calc_date = %sysfunc(mdy(&age_date.));
		if EL_DOB ne . then age = INT(YRDIF(EL_DOB, calc_date,'ACTUAL'));
		else age = .;
		
		if age = . then age_cat = .;
		else if age>=0 and age<=18 then age_cat=1;
		else if age>=19 and age<=64 then age_cat=2;
		else if age>=65 then age_cat=3;

		if age=0 then age_0=1;
		else age_0 = 0;
		if age>=1 and age<=5 then age_1_5=1;
		else age_1_5 = 0;
		if age>=6 and age<=18 then age_6_18=1;
		else age_6_18 = 0;
		if age>=19 and age<=44 then age_19_44=1;
		else age_19_44 = 0;
		if age>=45 and age<=64 then age_45_64=1;
		else age_45_64 = 0;
		if age>=65 and age <=84 then age_65_84=1;
		else age_65_84 = 0;
		if age>=85 then age_85p=1;
		else age_85p = 0;

		/* recode gender */
		if el_sex_cd='M' then male=1;
		else if el_sex_cd='F' then male=0;

		/*latest_elg_code = coalescec(MAX_ELG_CD_MO_12, MAX_ELG_CD_MO_11, MAX_ELG_CD_MO_10, MAX_ELG_CD_MO_9, MAX_ELG_CD_MO_8, MAX_ELG_CD_MO_7, MAX_ELG_CD_MO_6, MAX_ELG_CD_MO_5, 
		MAX_ELG_CD_MO_4, MAX_ELG_CD_MO_3, MAX_ELG_CD_MO_2, MAX_ELG_CD_MO_1);*/

		latest_elg_code = EL_MAX_ELGBLTY_CD_LTST; /*decided to use LTST code because it is the last/best code and it was used previously*/

		/* mas */
		if substr(latest_elg_code,1,1)="1" then mas_cash=1;
		else if missing(latest_elg_code) then mas_cash = .;
		else mas_cash=0;

		if substr(latest_elg_code,1,1)="2" then mas_mn=1;
		else if missing(latest_elg_code) then mas_mn = .;
		else mas_mn=0;

		if substr(latest_elg_code,1,1)="3" then mas_pov=1;
		else if missing(latest_elg_code) then mas_pov = .;
		else mas_pov=0;

		if substr(latest_elg_code,1,1)="4" and latest_elg_code~="48" then mas_oth=1;
		else if missing(latest_elg_code) then mas_oth = .;
		else mas_oth=0;

		if substr(latest_elg_code,1,1)="5" then mas_1115=1;
		else if missing(latest_elg_code) then mas_1115 = .;
		else mas_1115=0;

		/* boe */
		if substr(latest_elg_code,2,1)="1" then boe_aged=1;
		else if missing(latest_elg_code) then boe_aged = .;
		else boe_aged=0;

		if substr(latest_elg_code,2,1)="2" then boe_disabled=1;
		else if missing(latest_elg_code) then boe_disabled = .;
		else boe_disabled=0;

		if substr(latest_elg_code,2,1)="4" then boe_child=1;
		else if missing(latest_elg_code) then boe_child = .;
		else boe_child=0;

		if substr(latest_elg_code,2,1)="5" then boe_adult=1;
		else if missing(latest_elg_code) then boe_adult = .;
		else boe_adult=0;

		if substr(latest_elg_code,2,1)="6" then boe_uchild=1;
		else if missing(latest_elg_code) then boe_uchild = .;
		else boe_uchild=0;

		if substr(latest_elg_code,2,1)="7" then boe_uadult=1;
		else if missing(latest_elg_code) then boe_uadult = .;
		else boe_uadult=0;

		if substr(latest_elg_code,2,1)="8" then boe_fchild=1;
		else if missing(latest_elg_code) then boe_fchild = .;
		else boe_fchild=0;
		
		/*disability*/
		if EL_MAX_ELGBLTY_CD_LTST in ("12","22","32","42","52") then disabled = 1;
		else if EL_MAX_ELGBLTY_CD_LTST~="99" and not missing(EL_MAX_ELGBLTY_CD_LTST) then disabled = 0;

		/**************************************************/
	  	/*geo corrections								  */
		/*additional geo corrections follow this data step*/
		/**************************************************/
		* FL correction;
		if state_cd='FL' and EL_RSDNC_CNTY_CD_LTST='025' then EL_RSDNC_CNTY_CD_LTST='086';
		* NV correction;
		if state_cd='NV' then do;
			if EL_RSDNC_CNTY_CD_LTST='703' or EL_RSDNC_CNTY_CD_LTST='803' then EL_RSDNC_CNTY_CD_LTST='003';
			if EL_RSDNC_CNTY_CD_LTST='731' or EL_RSDNC_CNTY_CD_LTST='831' then EL_RSDNC_CNTY_CD_LTST='031';
		end;
		* NY correction;
		if state_cd='NY' and EL_RSDNC_CNTY_CD_LTST="005" then EL_RSDNC_CNTY_CD_LTST="061";
		*AK correction;
		if state_cd='AK' and EL_RSDNC_CNTY_CD_LTST="270" then EL_RSDNC_CNTY_CD_LTST="158";
		*SD correction;
		if state_cd='SD' and EL_RSDNC_CNTY_CD_LTST="113" then EL_RSDNC_CNTY_CD_LTST="102";

		if EL_RSDNC_CNTY_CD_LTST not in('000','999',' ') then st_cnty=catx('-',STATE_CD,EL_RSDNC_CNTY_CD_LTST);

		if EL_RSDNC_ZIP_CD_LTST = 00000 then zip_miss = .;
		else zip_miss = EL_RSDNC_ZIP_CD_LTST;

		if EL_RSDNC_CNTY_CD_LTST in('000','999') then county_miss = ' ';
		else county_miss = EL_RSDNC_CNTY_CD_LTST;

		/***********************/
	    /*create missing flags */
		/***********************/
		if missing(age) then _age=1;
		else _age=0;

		if missing(male) then _male=1;
		else _male=0;

		if missing(died_n) then _died=1;
		else _died=0;

		/* create death flag */
		if missing(EL_DOD) and missing(MDCR_DOD) then died_n=0;
		else died_n=1;
	run;

	/*macro that creates a macro variable list that contains dummy variables given a monthly input var(month_var), a valid condition (condition_one), a zero condition (condition_zero), the prefix for the dummary variables (dum_var_pref), and the macro variable name (macro_var_name)*/
	%macro dum_var(indata,month_var,condition_one,condition_zero,dum_var_pref,macro_var_name);
		proc sql noprint;
			select 'case when ' || name || &condition_one. || " then 1 when " || name || &condition_zero. || " then 0 else . end as &dum_var_pref." || substr(name,length(name)-1,2) into :&&macro_var_name. separated by ", "
			from dictionary.columns
			where libname="WORK" and memname = upcase("&indata.") and name like "&month_var._%";
		quit;
	%mend;
	%let mc_dum=;
	%dum_var(indata=temp1,
		month_var=MC_COMBO_MO_,
		condition_one="in (01,06,07,08,09)",
		condition_zero="not in (99,.)",
		dum_var_pref=mc_,
		macro_var_name=mc_dum);
	%let mc_dum_missing=;
	%dum_var(indata=temp1,
		month_var=MC_COMBO_MO_,
		condition_one="in (99,.)",
		condition_zero="not in (99,.)",
		dum_var_pref=_mc_,
		macro_var_name=mc_dum_missing);
	%let dual_dum=;
	%dum_var(indata=temp1,
		month_var=EL_MDCR_DUAL_MO_,
		condition_one="in ('01','02','03','04','05','06','07','08','09','51','52','53','54','55','56','57','58','59')",
		condition_zero="in ('00','50')",
		dum_var_pref=dual_,
		macro_var_name=dual_dum);
	%let dual_dum_missing=;
	%dum_var(indata=temp1,
		month_var=EL_MDCR_DUAL_MO_,
		condition_one="in ('98','99','')",
		condition_zero="not in ('98','99','')",
		dum_var_pref=_dual_,
		macro_var_name=dual_dum_missing);
	%let dual_full_dum=;
	%dum_var(indata=temp1,
		month_var=EL_MDCR_DUAL_MO_,
		condition_one="in ('09','59','00','50')",
		condition_zero="not in ('09','59','00','50')",
		dum_var_pref=dual_full_,
		macro_var_name=dual_full_dum);
	%let dual_full_dum_missing=;
	%dum_var(indata=temp1,
		month_var=EL_MDCR_DUAL_MO_,
		condition_one="in ('09','59','00','50')",
		condition_zero="not in ('09','59','00','50')",
		dum_var_pref=_dual_full_,
		macro_var_name=dual_full_dum_missing);
	%let disabled_dum=;
	%dum_var(indata=temp1,
		month_var=MAX_ELG_CD_MO_,
		condition_one="in ('12','22','32','42','52')",
		condition_zero="not in ('99','')",
		dum_var_pref=disabled_,
		macro_var_name=disabled_dum);
	%let chip_dum=;
	%dum_var(indata=temp1,
		month_var=EL_CHIP_FLAG_,
		condition_one="in (2,3)",
		condition_zero="not in (9,.)",
		dum_var_pref=chip_,
		macro_var_name=chip_dum);
	%let chip_missing_dum=;
	%dum_var(indata=temp1,
		month_var=EL_CHIP_FLAG_,
		condition_one="in (9,.)",
		condition_zero="not in (9,.)",
		dum_var_pref=_chip_,
		macro_var_name=chip_missing_dum);
	%let nmcd_dum=;
	%dum_var(indata=temp1,
		month_var=MAX_ELG_CD_MO_,
		condition_one="= '00'",
		condition_zero="ne '00'",
		dum_var_pref=nmcd_,
		macro_var_name=nmcd_dum);
	%let foster_dum=;
	%dum_var(indata=temp1,
		month_var=MAX_ELG_CD_MO_,
		condition_one="= '48'",
		condition_zero="ne '48'",
		dum_var_pref=foster_,
		macro_var_name=foster_dum);
	proc sql;
		create table temp2 as 
		select *,
			 &mc_dum.,
			 &mc_dum_missing.,
			 &dual_dum.,
			 &dual_dum_missing.,
			 &dual_full_dum.,
			 &dual_full_dum_missing.,
			 &disabled_dum.,
			 &chip_dum.,
			 &chip_missing_dum.,
			 &nmcd_dum.,
			 &foster_dum.
		from temp1;
	quit;

	%macro create_servicetype_cat(month_suff=);
		case 
			when foster_&month_suff. = 1 then "17"
			when nmcd_&month_suff. = 1 then "nmcd"
			when chip_&month_suff. = 1 then "chip"
			when dual_&month_suff. = 0 and mc_&month_suff.=1 and disabled_&month_suff.=1 and ltss=1 then "01" 
			when dual_&month_suff. = 0 and mc_&month_suff.=1 and disabled_&month_suff.=1 and ltss=0 then "02" 
			when dual_&month_suff. = 0 and mc_&month_suff.=1 and disabled_&month_suff.=0 and ltss=1 then "03" 
			when dual_&month_suff. = 0 and mc_&month_suff.=1 and disabled_&month_suff.=0 and ltss=0 then "04" 
			when dual_&month_suff. = 0 and mc_&month_suff.=0 and disabled_&month_suff.=1 and ltss=1 then "05" 
			when dual_&month_suff. = 0 and mc_&month_suff.=0 and disabled_&month_suff.=1 and ltss=0 then "06" 
			when dual_&month_suff. = 0 and mc_&month_suff.=0 and disabled_&month_suff.=0 and ltss=1 then "07" 
			when dual_&month_suff. = 0 and mc_&month_suff.=0 and disabled_&month_suff.=0 and ltss=0 then "08" 
			when dual_&month_suff. = 1 and mc_&month_suff.=1 and disabled_&month_suff.=1 and ltss=1 then "09" 
			when dual_&month_suff. = 1 and mc_&month_suff.=1 and disabled_&month_suff.=1 and ltss=0 then "10" 
			when dual_&month_suff. = 1 and mc_&month_suff.=1 and disabled_&month_suff.=0 and ltss=1 then "11" 
			when dual_&month_suff. = 1 and mc_&month_suff.=1 and disabled_&month_suff.=0 and ltss=0 then "12" 
			when dual_&month_suff. = 1 and mc_&month_suff.=0 and disabled_&month_suff.=1 and ltss=1 then "13" 
			when dual_&month_suff. = 1 and mc_&month_suff.=0 and disabled_&month_suff.=1 and ltss=0 then "14" 
			when dual_&month_suff. = 1 and mc_&month_suff.=0 and disabled_&month_suff.=0 and ltss=1 then "15" 
			when dual_&month_suff. = 1 and mc_&month_suff.=0 and disabled_&month_suff.=0 and ltss=0 then "16" 
			else "" 
		end as servicetype_&month_suff.
	%mend;

	proc sql;
		create table temp3 as 
		select *, 
			%create_servicetype_cat(month_suff=_1),
			%create_servicetype_cat(month_suff=_2),
			%create_servicetype_cat(month_suff=_3),
			%create_servicetype_cat(month_suff=_4),
			%create_servicetype_cat(month_suff=_5),
			%create_servicetype_cat(month_suff=_6),
			%create_servicetype_cat(month_suff=_7),
			%create_servicetype_cat(month_suff=_8),
			%create_servicetype_cat(month_suff=_9),
			%create_servicetype_cat(month_suff=10),
			%create_servicetype_cat(month_suff=11),
			%create_servicetype_cat(month_suff=12)
		from temp2;
	quit;

	/*macro that creates a macro variable list that contains dummy variables given a monthly input var(month_var), a valid condition (condition_one), a zero condition (condition_zero), the prefix for the dummary variables (dum_var_pref), and the macro variable name (macro_var_name)*/
	%macro count_var(indata,month_var,condition_one,macro_var_name);
		proc sql noprint;
			select 'case when ' || name || &condition_one.||" then 1 else 0 end" into :&&macro_var_name. separated by ", "
			from dictionary.columns
			where libname="WORK" and memname = upcase("&indata.") and name like "&month_var._%";
		quit;
	%mend;

	%let count_01=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('01')",macro_var_name=count_01);
	%let count_02=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('02')",macro_var_name=count_02);
	%let count_03=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('03')",macro_var_name=count_03);
	%let count_04=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('04')",macro_var_name=count_04);
	%let count_05=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('05')",macro_var_name=count_05);
	%let count_06=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('06')",macro_var_name=count_06); 
	%let count_07=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('07')",macro_var_name=count_07);
	%let count_08=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('08')",macro_var_name=count_08);
	%let count_09=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('09')",macro_var_name=count_09);
	%let count_10=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('10')",macro_var_name=count_10);
	%let count_11=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('11')",macro_var_name=count_11);
	%let count_12=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('12')",macro_var_name=count_12);
	%let count_13=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('13')",macro_var_name=count_13);
	%let count_14=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('14')",macro_var_name=count_14);
	%let count_15=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('15')",macro_var_name=count_15);
	%let count_16=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('16')",macro_var_name=count_16);
	%let count_17=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('17')",macro_var_name=count_17);
	%let count_mcd_only=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('01','02','03','04','05','06','07','08')",macro_var_name=count_mcd_only);
	%let count_dual_only=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('09','10','11','12','13','14','15','16')",macro_var_name=count_dual_only);
	%let count_mc_only=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('01','02','03','04','09','10','11','12')",macro_var_name=count_mc_only);
	%let count_ffs_only=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('05','06','07','08','13','14','15','16')",macro_var_name=count_ffs_only);
	%let count_dsbl=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('01','02','05','06','09','10','13','14')",macro_var_name=count_dsbl);
	%let count_nondsbl=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('03','04','07','08','11','12','15','16')",macro_var_name=count_nondsbl);
	%let count_ltss=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('01','03','05','07','09','11','13','15')",macro_var_name=count_ltss);
	%let count_nonltss=;%count_var(indata=temp3,month_var=servicetype,condition_one="in ('02','04','06','08','10','12','14','16')",macro_var_name=count_nonltss);
	
	proc sql;
		create table temp4 as
		select *,
			sum(&count_01.) as elig_months_01,
			sum(&count_02.) as elig_months_02,
			sum(&count_03.) as elig_months_03,
			sum(&count_04.) as elig_months_04,
			sum(&count_05.) as elig_months_05,
			sum(&count_06.) as elig_months_06,
			sum(&count_07.) as elig_months_07,
			sum(&count_08.) as elig_months_08,
			sum(&count_09.) as elig_months_09,
			sum(&count_10.) as elig_months_10,
			sum(&count_11.) as elig_months_11,
			sum(&count_12.) as elig_months_12,
			sum(&count_13.) as elig_months_13,
			sum(&count_14.) as elig_months_14,
			sum(&count_15.) as elig_months_15,
			sum(&count_16.) as elig_months_16,
			sum(&count_17.) as elig_months_17,
			sum(&count_mcd_only.) as mo_mcd,
			sum(&count_dual_only.) as mo_dual,
			sum(&count_mc_only.) as mo_mc,
			sum(&count_ffs_only.) as mo_ffs,
			sum(&count_dsbl.) as mo_dsbl,
			sum(&count_nondsbl.) as mo_nondsbl,
			sum(&count_ltss.) as mo_ltss,
			sum(&count_nonltss.) as mo_nonltss
		from temp3 (drop=cell_age);
	quit;
	/*macro that creates a macro variable list that contains dummy variables given a monthly input var(month_var), a valid condition (condition_one), a zero condition (condition_zero), the prefix for the dummary variables (dum_var_pref), and the macro variable name (macro_var_name)*/

	data &outdata.;
		set temp4;
		length age_servicetype $16.;
			/* create # of month for each cell variables */
			array eligmo_servicetypes{*} 3. months_1-months_17;
			do i=1 to 17;
				eligmo_servicetypes(i) = 0;
			end;
			array x{17} $ x1-x17 ("01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12" "13" "14" "15" "16" "17" );
			array months servicetype__1 servicetype__2 servicetype__3 servicetype__4 servicetype__5 servicetype__6 servicetype__7 servicetype__8 servicetype__9 servicetype_11 servicetype_10 servicetype_11 servicetype_12;
			do i=1 to 17;
				do j=1 to 12;
					if months(j) = x(i) then eligmo_servicetypes(i)=eligmo_servicetypes(i)+1;
				end;
			end;
			/*calculate number of distinct MAS/BOE categories a bene_id falls into (including nmcd, msg, and chip)*/
			do i=1 to 12;
		    	if not missing(months(i)) then do;
		      		if i lt 12 then do;
		        		if months(i) ~= months(i+1) then d_servicetype_n=sum(d_servicetype_n,1);
		      		end;
		      		else d_servicetype_n=sum(d_servicetype_n,1);
		   		 end;
		  	end;

			drop x1-x17 i j;

			if servicetype_12 not in ('chip','nmcd','') then latest_servicetype = coalescec(servicetype_12,servicetype_11,servicetype_10,servicetype__9,servicetype__8,servicetype__7,
																		servicetype__6,servicetype__5,servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype_11 not in ('chip','nmcd','') then latest_servicetype = coalescec(servicetype_11,servicetype_10,servicetype__9,servicetype__8,servicetype__7,
																		servicetype__6,servicetype__5,servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype_10 not in ('chip','nmcd','') then latest_servicetype = coalescec(servicetype_10,servicetype__9,servicetype__8,servicetype__7,
																		servicetype__6,servicetype__5,servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype__9 not in ('chip','nmcd','') then latest_servicetype = coalescec(servicetype__9,servicetype__8,servicetype__7,
																		servicetype__6,servicetype__5,servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype__8 not in ('chip','nmcd','') then latest_servicetype = coalescec(servicetype__8,servicetype__7,
																		servicetype__6,servicetype__5,servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype__7 not in ('chip','nmcd','') then latest_servicetype = coalescec(servicetype__7,
																		servicetype__6,servicetype__5,servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype__6 not in ('chip','nmcd','') then latest_servicetype = coalescec(servicetype__6,servicetype__5,servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype__5 not in ('chip','nmcd','') then latest_servicetype = coalescec(servicetype__5,servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype__4 not in ('chip','nmcd','') then latest_servicetype = coalescec(servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype__3 not in ('chip','nmcd','') then latest_servicetype = coalescec(servicetype__3,servicetype__2,servicetype__1);
			else if servicetype__2 not in ('chip','nmcd','') then latest_servicetype = coalescec(servicetype__2,servicetype__1);
			else if servicetype__1 not in ('chip','nmcd','') then latest_servicetype = servicetype__1;
			
			if age_cat=1 and not missing(latest_servicetype) then age_servicetype = catx('_','child',latest_servicetype); 
			else if age_cat=2 and not missing(latest_servicetype) then age_servicetype = catx('_','adult',latest_servicetype); 
			else if age_cat=3 and not missing(latest_servicetype) then age_servicetype = catx('_','senior',latest_servicetype);

			if latest_servicetype='01' then do; 			dual_cat=0; mc_cat=1; dis_cat=1; ltss_cat=1; foster_cat=0;end; 
			else if latest_servicetype='02' then do; 		dual_cat=0; mc_cat=1; dis_cat=1; ltss_cat=0; foster_cat=0;end; 
			else if latest_servicetype='03' then do; 		dual_cat=0; mc_cat=1; dis_cat=0; ltss_cat=1; foster_cat=0;end; 
			else if latest_servicetype='04' then do; 		dual_cat=0; mc_cat=1; dis_cat=0; ltss_cat=0; foster_cat=0;end; 
			else if latest_servicetype='05' then do; 		dual_cat=0; mc_cat=0; dis_cat=1; ltss_cat=1; foster_cat=0;end; 
			else if latest_servicetype='06' then do; 		dual_cat=0; mc_cat=0; dis_cat=1; ltss_cat=0; foster_cat=0;end; 
			else if latest_servicetype='07' then do; 		dual_cat=0; mc_cat=0; dis_cat=0; ltss_cat=1; foster_cat=0;end; 
			else if latest_servicetype='08' then do; 		dual_cat=0; mc_cat=0; dis_cat=0; ltss_cat=0; foster_cat=0;end; 
			else if latest_servicetype='09' then do; 		dual_cat=1; mc_cat=1; dis_cat=1; ltss_cat=1; foster_cat=0;end; 
			else if latest_servicetype='10' then do; 		dual_cat=1; mc_cat=1; dis_cat=1; ltss_cat=0; foster_cat=0;end; 
			else if latest_servicetype='11' then do;		dual_cat=1; mc_cat=1; dis_cat=0; ltss_cat=1; foster_cat=0;end; 
			else if latest_servicetype='12' then do;		dual_cat=1; mc_cat=1; dis_cat=0; ltss_cat=0; foster_cat=0;end; 
			else if latest_servicetype='13' then do;		dual_cat=1; mc_cat=0; dis_cat=1; ltss_cat=1; foster_cat=0;end; 
			else if latest_servicetype='14' then do;		dual_cat=1; mc_cat=0; dis_cat=1; ltss_cat=0; foster_cat=0;end; 
			else if latest_servicetype='15' then do;		dual_cat=1; mc_cat=0; dis_cat=0; ltss_cat=1; foster_cat=0;end; 
			else if latest_servicetype='16' then do;		dual_cat=1; mc_cat=0; dis_cat=0; ltss_cat=0; foster_cat=0;end; 
			else if latest_servicetype='17' then do;		age_servicetype = "child_17"; age_cat = 1; dual_cat=9; mc_cat=9; dis_cat=9; ltss_cat=9; foster_cat=1; end;
			else if latest_servicetype='chip' then do;		dual_cat=8; mc_cat=8; dis_cat=8; ltss_cat=8; foster_cat=8; end;
			else if latest_servicetype='nmcd' then do;		dual_cat=7; mc_cat=7; dis_cat=7; ltss_cat=7; foster_cat=7; end;
			else if latest_servicetype='' then do;		dual_cat=6; mc_cat=6; dis_cat=6; ltss_cat=6; foster_cat=6; end;

			cell_type1 = '';
			if missing(latest_servicetype) then cell_type1 = 'msg';
			else if latest_servicetype in ('01','02','03','04','05','06','07','08') then cell_type1 = "Medicaid Only";
			else if latest_servicetype not in ('01','02','03','04','05','06','07','08','17') then cell_type1 = "Dual";
			else if latest_servicetype = '17' then cell_type1 = 'Foster Care';
			
			cell_type2 = ' ';
			if missing(latest_servicetype) then cell_type2 = 'msg';
			else if latest_servicetype in ('01','02','03','04','09','10','11','12')  then cell_type2 = "Managed Care";
			else if latest_servicetype not in ('01','02','03','04','09','10','11','12','17') then cell_type2 = "Fee-For-Service";
			else if latest_servicetype = '17' then cell_type2 = 'Foster Care';

			cell_type3 = '';
			if missing(latest_servicetype) then cell_type3 = 'msg';
			else if latest_servicetype in ('01','02','05','06','09','10','13','14') then cell_type3 = "Disability";
			else if latest_servicetype not in ('01','02','05','06','09','10','13','14','17') then cell_type3 = "No Disability";
			else if latest_servicetype = '17' then cell_type3 = 'Foster Care';

			cell_type4 = '';
			if missing(latest_servicetype) then cell_type4 = 'msg';
			else if latest_servicetype in ('01','03','05','07','09','11','13','15') then cell_type4 = "LTSS";
			else if latest_servicetype not in ('01','03','05','07','09','11','13','15','17') then cell_type4 = "No LTSS";
			else if latest_servicetype = '17' then cell_type4 = 'Foster Care';

			label 	
				_age = "Missing age flag"
				_chip__1 = "Missing or unknown chip_1 flag"
				_chip__2 = "Missing or unknown chip_2 flag"
				_chip__3 = "Missing or unknown chip_3 flag"
				_chip__4 = "Missing or unknown chip_4 flag"
				_chip__5 = "Missing or unknown chip_5 flag"
				_chip__6 = "Missing or unknown chip_6 flag"
				_chip__7 = "Missing or unknown chip_7 flag"
				_chip__8 = "Missing or unknown chip_8 flag"
				_chip__9 = "Missing or unknown chip_9 flag"
				_chip_10 = "Missing or unknown chip_10 flag"
				_chip_11 = "Missing or unknown chip_11 flag"
				_chip_12 = "Missing or unknown chip_12 flag"
				_died = "Missing or unknown died flag"
				_dual__1 = "Missing or unknown dual_1 flag"
				_dual__2 = "Missing or unknown dual_2 flag"
				_dual__3 = "Missing or unknown dual_3 flag"
				_dual__4 = "Missing or unknown dual_4 flag"
				_dual__5 = "Missing or unknown dual_5 flag"
				_dual__6 = "Missing or unknown dual_6 flag"
				_dual__7 = "Missing or unknown dual_7 flag"
				_dual__8 = "Missing or unknown dual_8 flag"
				_dual__9 = "Missing or unknown dual_9 flag"
				_dual_10 = "Missing or unknown dual_10 flag"
				_dual_11 = "Missing or unknown dual_11 flag"
				_dual_12 = "Missing or unknown dual_12 flag"
				_dual_full__1 = "Missing or unknown dual_full_1 flag"
				_dual_full__2 = "Missing or unknown dual_full_2 flag"
				_dual_full__3 = "Missing or unknown dual_full_3 flag"
				_dual_full__4 = "Missing or unknown dual_full_4 flag"
				_dual_full__5 = "Missing or unknown dual_full_5 flag"
				_dual_full__6 = "Missing or unknown dual_full_6 flag"
				_dual_full__7 = "Missing or unknown dual_full_7 flag"
				_dual_full__8 = "Missing or unknown dual_full_8 flag"
				_dual_full__9 = "Missing or unknown dual_full_9 flag"
				_dual_full_10 = "Missing or unknown dual_full_10 flag"
				_dual_full_11 = "Missing or unknown dual_full_11 flag"
				_dual_full_12 = "Missing or unknown dual_full_12 flag"
				_ltss = "Missing or unknown ltss flag"
				_male = "Missing or unknown male flag"
				_mc__1 = "Missing or unknown mc1 flag"
				_mc__2 = "Missing or unknown mc2 flag"
				_mc__3 = "Missing or unknown mc3 flag"
				_mc__4 = "Missing or unknown mc4 flag"
				_mc__5 = "Missing or unknown mc5 flag"
				_mc__6 = "Missing or unknown mc6 flag"
				_mc__7 = "Missing or unknown mc7 flag"
				_mc__8 = "Missing or unknown mc8 flag"
				_mc__9 = "Missing or unknown mc9 flag"
				_mc_10 = "Missing or unknown mc10 flag"
				_mc_11 = "Missing or unknown mc11 flag"
				_mc_12 = "Missing or unknown mc12 flag"
				age = "Age"
				age_0 = "Less than 1 year old as of 12/1/2012"
				age_19_44 = "Between 19-44 years old as of 12/1/2012"
				age_1_5 = "Between 1-5 years old  as of 12/1/2012"
				age_45_64 = "Between 45-64 years old  as of 12/1/2012"
				age_65_84 = "Between 65-84 years old  as of 12/1/2012"
				age_6_18 = "Between 6-18 years old as of 12/1/2012"
				age_85p = "85 years or older as of 12/1/2012"
				age_cat ="Age Category"
				boe_adult ="BOE Adult Beneficiary"
				boe_aged ="BOE Aged Beneficiary"
				boe_child ="BOE Child Beneficiary"
				boe_disabled ="BOE Disabled Beneficiary"
				boe_fchild ="BOE Foster Child Beneficiary"
				boe_uadult ="BOE Unemployed Adult Beneficiary"
				boe_uchild ="BOE Child (Unemployed Adult) Beneficiary"
				cell_n = "Flag for sums"
				cell = "Latest known MAS/BOE/Foster category"
				cell_age = "Cell and age category"
				cell_type1 = "Medicaid only or dual"
				cell_type2 = "Managed care or FFS"
				cell_type3 = "Disability or no disability"
				cell_type4 = "LTSS or no LTSS"
				chip__1 = "CHIP - Jan"
				chip__2 = "CHIP - Feb"
				chip__3 = "CHIP - Mar"
				chip__4 = "CHIP - Apr"
				chip__5 = "CHIP - May"
				chip__6 = "CHIP - Jun"
				chip__7 = "CHIP - Jul"
				chip__8 = "CHIP - Aug"
				chip__9 = "CHIP - Sep"
				chip_10 = "CHIP - Oct"
				chip_11 = "CHIP - Nov"
				chip_12 = "CHIP - Dec"
				cltc = "Sum of CLTC_FFS_PYMT_AMT_11-CLTC_FFS_PYMT_AMT_40"
				d_cell_n ="Number of Unique Statuses"
				died_n ="Number Dying in Year"
				dis_cat ="Disability Category"
				disabled__1 = "Disabled - Jan"
				disabled__2 = "Disabled - Feb"
				disabled__3 = "Disabled - Mar"
				disabled__4 = "Disabled - Apr"
				disabled__5 = "Disabled - May"
				disabled__6 = "Disabled - Jun"
				disabled__7 = "Disabled - Jul"
				disabled__8 = "Disabled - Aug"
				disabled__9 = "Disabled - Sep"
				disabled_10 = "Disabled - Oct"
				disabled_11 = "Disabled - Nov"
				disabled_12 = "Disabled - Dec"
				dual__1 = "Dual eligible - Jan"
				dual__2 = "Dual eligible - Feb"
				dual__3 = "Dual eligible - Mar"
				dual__4 = "Dual eligible - Apr"
				dual__5 = "Dual eligible - May"
				dual__6 = "Dual eligible - Jun"
				dual__7 = "Dual eligible - Jul"
				dual__8 = "Dual eligible - Aug"
				dual__9 = "Dual eligible - Sep"
				dual_10 = "Dual eligible - Oct"
				dual_11 = "Dual eligible - Nov"
				dual_12 = "Dual eligible - Dec"
				dual_full__1 = "Full dual eligible - Jan"
				dual_full__2 = "Full dual eligible - Feb"
				dual_full__3 = "Full dual eligible - Mar"
				dual_full__4 = "Full dual eligible - Apr"
				dual_full__5 = "Full dual eligible - May"
				dual_full__6 = "Full dual eligible - Jun"
				dual_full__7 = "Full dual eligible - Jul"
				dual_full__8 = "Full dual eligible - Aug"
				dual_full__9 = "Full dual eligible - Sep"
				dual_full_10 = "Full dual eligible - Oct"
				dual_full_11 = "Full dual eligible - Nov"
				dual_full_12 = "Full dual eligible - Dec"
				foster__1 = "Foster care - Jan"
				foster__2 = "Foster care - Feb"
				foster__3 = "Foster care - Mar"
				foster__4 = "Foster care - Apr"
				foster__5 = "Foster care - May"
				foster__6 = "Foster care - Jun"
				foster__7 = "Foster care - Jul"
				foster__8 = "Foster care - Aug"
				foster__9 = "Foster care - Sep"
				foster_10 = "Foster care - Oct"
				foster_11 = "Foster care - Nov"
				foster_12 = "Foster care - Dec"
				elig_months_01 = "Eligible months for service type 1"
				elig_months_02 = "Eligible months for service type 2"
				elig_months_03 = "Eligible months for service type 3"
				elig_months_04 = "Eligible months for service type 4"
				elig_months_05 = "Eligible months for service type 5"
				elig_months_06 = "Eligible months for service type 6"
				elig_months_07 = "Eligible months for service type 7"
				elig_months_08 = "Eligible months for service type 8"
				elig_months_09 = "Eligible months for service type 9"
				elig_months_10 = "Eligible months for service type 10"
				elig_months_11 = "Eligible months for service type 11"
				elig_months_12 = "Eligible months for service type 12"
				elig_months_13 = "Eligible months for service type 13"
				elig_months_14 = "Eligible months for service type 14"
				elig_months_15 = "Eligible months for service type 15"
				elig_months_16 = "Eligible months for service type 16"
				elig_months_17 = "Eligible months for service type 17"
				dual_cat ="Dual-Eligibility Category"
				foster_cat ="Foster Care Category"
				last_mo ="Last month with known MAS/BOE/Foster care category "
				lt_nurs = "Fee_for_srvc_ind_07 recipient"
				ltss = "Long-term support services"
				ltss_cat ="LTSS Use Category"
				male ="Number of Male Beneficiary"
				mas_1115 ="MAS 1115 Exspansion Beneficiary"
				mas_cash ="MAS Cash Beneficiary"
				mas_mn ="MAS Medically Needy Beneficiary"
				mas_oth ="MAS Other Beneficiary"
				mas_pov ="MAS Poverty-Related Beneficiary"
				mc__1 = "Managed care - Jan"
				mc__2 = "Managed care - Feb"
				mc__3 = "Managed care - Mar"
				mc__4 = "Managed care - Apr"
				mc__5 = "Managed care - May"
				mc__6 = "Managed care - Jun"
				mc__7 = "Managed care - Jul"
				mc__8 = "Managed care - Aug"
				mc__9 = "Managed care - Sep"
				mc_10 = "Managed care - Oct"
				mc_11 = "Managed care - Nov"
				mc_12 = "Managed care - Dec"
				mc_cat ="Managed Care Category"
				mo_mcd ="Months eligible for Medicaid"
				mo_dsbl ="Months eligible for disability"
				mo_dsbl ="Months not eligible for disability"
				mo_dual ="Months eligible for dual"
				mo_ffs ="Months eligible for FFS"
				mo_ltss ="Months eligible for long-term support services"
				mo_mc ="Months eligible for managed care"
				nmcd__1 = "Not Mediciad - Jan"
				nmcd__2 = "Not Mediciad - Feb"
				nmcd__3 = "Not Mediciad - Mar"
				nmcd__4 = "Not Mediciad - Apr"
				nmcd__5 = "Not Mediciad - May"
				nmcd__6 = "Not Mediciad - Jun"
				nmcd__7 = "Not Mediciad - Jul"
				nmcd__8 = "Not Mediciad - Aug"
				nmcd__9 = "Not Mediciad - Sep"
				nmcd_10 = "Not Mediciad - Oct"
				nmcd_11 = "Not Mediciad - Nov"
				nmcd_12 = "Not Mediciad - Dec"
				pm_n = "Eligible months"
				st_cnty = "State abbreviation and county code"
				state_cd = "State abbreviation"
				tot_pay ="TOT_MDCD_PYMT_AMT "
				year = "Claims year"
				recipno = "Recipient number for linking CDPS data"
				TOT_IP_STAY_CNT = "Inpatient Stays Count"
				TOT_IP_DAY_CNT_STAYS = "Inpatient Days Count"
				FFS_PYMT_AMT_01 = "Inpatient Spending"
				physclin_clm_cnt = "Physician and clinic claim count"
				physclin_pymt_amt = "Physician and clinic spending"
				FFS_CLM_CNT_16 = "Prescription drug claims count"
				FFS_PYMT_AMT_16 = "Prescription drug spending"
				full_benf_ltst = "Bene benefits statusin the last observed month"
				full_benf = "In the last observed month, beneficiary had: full benefits = 1, no benefits = 0, partial benefits = 99"
				partial_benefits_1 = "Beneficiary had partial benefits - Jan"
				partial_benefits_2 = "Beneficiary had partial benefits - Feb"
				partial_benefits_3 = "Beneficiary had partial benefits - Mar"
				partial_benefits_4 = "Beneficiary had partial benefits - Apr"
				partial_benefits_5 = "Beneficiary had partial benefits - May"
				partial_benefits_6 = "Beneficiary had partial benefits - Jun"
				partial_benefits_7 = "Beneficiary had partial benefits - Jul"
				partial_benefits_8 = "Beneficiary had partial benefits - Aug"
				partial_benefits_9 = "Beneficiary had partial benefits - Sep"
				partial_benefits_10 = "Beneficiary had partial benefits - Oct"
				partial_benefits_11 = "Beneficiary had partial benefits - Nov"
				partial_benefits_12 = "Beneficiary had partial benefits - Dec"
				;
		run;

	%mend;

%make_popfile(indata=data.maxdata_ps_2012, outdata=space.temp_personlevel_&space_name.);
