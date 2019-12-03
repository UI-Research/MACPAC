/*******************************************************************************************************************/ 
/*	Purpose: Using a yearly MAX input data set, create an analytic file with specified variables. 
/*				Fix invalid geographic information.
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)
/*	Notes: 
/*		1) Collapse macros for easier manipulation
/*		2) Dependent on MAX data formatting
/*******************************************************************************************************************/ 

* Macro variables for processing;
%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.)); /*date+time for file names, to ensure files aren't overwritten*/
%let space_name = %sysfunc(date(),date9.); /*date for naming convention in space lib*/
%let dum_tab = %str(ods excel options(sheet_interval="table"); ods exclude all; data _null_; file print; put _all_; run; ods select all;); /*adds a dummy table to make a new worksheet ods excel*/
%let report_folder = P:\MCD-SPVR\reports; /*location of output reports*/

/*options/macros to change*/
options obs=100000;
%let indata_year = 2012; /*claims year*/
%let age_date = 12,01,2012; /*date from which to calculate age. must be month,day,year*/

/*log
proc printto print="P:\MCD-SPVR\log\01_studypop_analyticfile_&fname..lst"
               log="P:\MCD-SPVR\log\01_studypop_analyticfile_&fname..log" new;
run;*/
/*libraries*/
libname data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;


proc format library=library;
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

/* macro to calculate month-specific variables */ 

%macro cells(indata,outdata);
	data limit_incoming_dat;
		set &indata. (keep=BENE_ID STATE_CD EL_RSDNC_ZIP_CD_LTST EL_RSDNC_CNTY_CD_LTST EL_DOB MC_COMBO_MO:
			EL_MDCR_DUAL_MO_: EL_RSTRCT_BNFT_FLG: MAX_ELG_CD_MO: EL_CHIP_FLAG: 
			EL_SEX_CD TOT_MDCD_PYMT_AMT EL_ELGBLTY_MO_CNT EL_MAX_ELGBLTY_CD_LTST
			EL_DOD MDCR_DOD CLTC: FEE_FOR_SRVC_IND_07 TOT_MDCD_PYMT_AMT );
		if missing(BENE_ID) then delete;
		if missing(EL_RSDNC_CNTY_CD_LTST) and EL_RSDNC_ZIP_CD_LTST = 00000 then delete;
	run;

	proc sql;
		create table space.unformatted_dropped_&space_name. as
		select a.*
		from &indata. (keep=BENE_ID STATE_CD EL_RSDNC_ZIP_CD_LTST EL_RSDNC_CNTY_CD_LTST EL_DOB MC_COMBO_MO:
			EL_MDCR_DUAL_MO_: EL_RSTRCT_BNFT_FLG: MAX_ELG_CD_MO: EL_CHIP_FLAG: 
			EL_SEX_CD TOT_MDCD_PYMT_AMT EL_ELGBLTY_MO_CNT EL_MAX_ELGBLTY_CD_LTST
			EL_DOD MDCR_DOD CLTC: FEE_FOR_SRVC_IND_07 TOT_MDCD_PYMT_AMT ) a 
			left join limit_incoming_dat b
		on a.BENE_ID = b.BENE_ID
		where b.BENE_ID is missing;
	quit;
	
	proc freq data=space.unformatted_dropped_&space_name.;
		title "Missing BENE_ID and Geo Info Unformatted Dropped File";
		tables BENE_ID*EL_RSDNC_CNTY_CD_LTST*EL_RSDNC_ZIP_CD_LTST/list missing;
		format BENE_ID EL_RSDNC_CNTY_CD_LTST $missing_char. EL_RSDNC_ZIP_CD_LTST missing_zip.;
	run;

	/* create person-level dataset with appropriate variables and cell types */
	data &outdata. ;
		format cell_type1-cell_type4 $16. cell $15. county_miss $3. zip_miss 5. cell_age $16. calc_date mmddyy8.;
		set limit_incoming_dat;
	
	  	/****************************/
		/*Variable creation/recoding*/
		/****************************/
		year = &indata_year.;
		cell_n = 1;
		pm_n = EL_ELGBLTY_MO_CNT;
		tot_pay = TOT_MDCD_PYMT_AMT;     
		recipno=catx ('_',STATE_CD,BENE_ID); /*to link the CDPS data*/

		/* LTSS */
		cltc = sum(of CLTC_FFS_PYMT_AMT_11--CLTC_FFS_PYMT_AMT_40);

		if Fee_for_srvc_ind_07=1 then lt_nurs=1;
		else lt_nurs=0;
		if missing(Fee_for_srvc_ind_07) then _ltss=1;

		if cltc>0 or lt_nurs=1 then ltss=1;
		if missing(Fee_for_srvc_ind_07) then ltss = .;
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

		latest_elg_code = EL_MAX_ELGBLTY_CD_LTST;

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

		/*******/
		/*Loops*/
		/*******/
	    /*determine cell value in each month*/
		/*each cell is a dictomous variable indicating 1) Medicaid only or Dual 2)Managed Care or FFS 3)Disability or No Disability and 4) LTSS or No LTSS */
		/*cell_17 indicates foster care*/
	     %do i=1 %to 12;			

			if MAX_ELG_CD_MO_&i. in ("11","12","14","15","16","17") then cash_&i.=1;
			else if MAX_ELG_CD_MO_&i.~="99" and not missing(MAX_ELG_CD_MO_&i.) then cash_&i.=0;
			
			if missing(MAX_ELG_CD_MO_&i.) or MAX_ELG_CD_MO_&i.="99" then _elig_&i.=1;
			else _elig_&i.=0;

			/*restricted benefits*/
			if EL_RSTRCT_BNFT_FLG_&i. = "1" or EL_MDCR_DUAL_MO_&i. in ("02","04","08","52","54","58") then mcd_full_&i.=1;
			else if EL_RSTRCT_BNFT_FLG_&i. ~="9" and not missing(EL_RSTRCT_BNFT_FLG_&i.) then mcd_full_&i.=0;

			if missing(EL_RSTRCT_BNFT_FLG_&i.) or EL_RSTRCT_BNFT_FLG_&i.="9" then _mcd_full_&i.=1;
			else _mcd_full_&i.=0;

			/* CHIP */
			if EL_CHIP_FLAG_&i. in(2,3)  then chip_&i.=1;
			else if EL_CHIP_FLAG_&i. ~=9 and not missing(EL_CHIP_FLAG_&i.)  then chip_&i.=0;

			if EL_CHIP_FLAG_&i.=9 or missing(EL_CHIP_FLAG_&i.) then _chip_&i.=1;
			else _chip_&i.=0;

			/*categories for cell definitions*/
			/* managed care */
			if MC_COMBO_MO_&i. in ("01","06","07","08","09") then mc_&i.=1;
			else if MC_COMBO_MO_&i. ~= "99" and not missing(MC_COMBO_MO_&i.) then mc_&i.=0;

			if MC_COMBO_MO_&i. = "99" or missing(MC_COMBO_MO_&i.) then _mc&i=1;
			else _mc&i=0;

			/* dual status */
			if EL_MDCR_DUAL_MO_&i. in("00","50") then dual_&i.=0;
			else if EL_MDCR_DUAL_MO_&i. in ("01","02","03","04","05","06","07","08","09","51","52","53","54","55","56","57","58","59") and not missing(EL_MDCR_DUAL_MO_&i.) then dual_&i.=1;

			if missing(EL_MDCR_DUAL_MO_&i.) or EL_MDCR_DUAL_MO_&i. in ("98","99") then _dual_&i.=1;
			else _dual_&i.=0;

			if EL_MDCR_DUAL_MO_&i. in ("09","59","00","50") then _dual_full_&i.=1;
			else _dual_full_&i.=0;

			/* disability status */	
			if MAX_ELG_CD_MO_&i. in ("12","22","32","42","52") then disabled_&i.=1;
			else if MAX_ELG_CD_MO_&i.~="99" and not missing(MAX_ELG_CD_MO_&i.) then disabled_&i.=0;

			/* create cells */
			if chip_&i.=1 then month_&i.="chip";
			else if MAX_ELG_CD_MO_&i.="00" then month_&i.="nmcd"; /*not medicaid*/
			/*else if _mc&i=1 or _dual_&i.=1 or (_dual_full_&i.=1 and _mcd_full_&i.=1) or _elig_&i.=1 or _chip_&i.=1 or _ltss=1 then month_&i.="msg"; /*missing*/
			else if MAX_ELG_CD_MO_&i.="48" then month_&i.="17"; 
			else if (dual_&i.=0 and mc_&i.=1 and disabled_&i.=1 and ltss=1) then month_&i. = "1";
			else if (dual_&i.=0 and mc_&i.=1 and disabled_&i.=1 and ltss=0) then month_&i. = "2";
			else if (dual_&i.=0 and mc_&i.=1 and disabled_&i.=0 and ltss=1) then month_&i. = "3";
			else if (dual_&i.=0 and mc_&i.=1 and disabled_&i.=0 and ltss=0) then month_&i. = "4";
			else if (dual_&i.=0 and mc_&i.=0 and disabled_&i.=1 and ltss=1) then month_&i. = "5";
			else if (dual_&i.=0 and mc_&i.=0 and disabled_&i.=1 and ltss=0) then month_&i. = "6";
			else if (dual_&i.=0 and mc_&i.=0 and disabled_&i.=0 and ltss=1) then month_&i. = "7";
			else if (dual_&i.=0 and mc_&i.=0 and disabled_&i.=0 and ltss=0) then month_&i. = "8";
			else if (dual_&i.=1 and mc_&i.=1 and disabled_&i.=1 and ltss=1) then month_&i. = "9";
			else if (dual_&i.=1 and mc_&i.=1 and disabled_&i.=1 and ltss=0) then month_&i. = "10";
			else if (dual_&i.=1 and mc_&i.=1 and disabled_&i.=0 and ltss=1) then month_&i. = "11";
			else if (dual_&i.=1 and mc_&i.=1 and disabled_&i.=0 and ltss=0) then month_&i. = "12";
			else if (dual_&i.=1 and mc_&i.=0 and disabled_&i.=1 and ltss=1) then month_&i. = "13";
			else if (dual_&i.=1 and mc_&i.=0 and disabled_&i.=1 and ltss=0) then month_&i. = "14";
			else if (dual_&i.=1 and mc_&i.=0 and disabled_&i.=0 and ltss=1) then month_&i. = "15";
			else if (dual_&i.=1 and mc_&i.=0 and disabled_&i.=0 and ltss=0) then month_&i. = "16";
			else month_&i. = "msg";
		%end;

		/*find latest month_[month] with data*/
		%do i=1 %to 12;
			if month_&i. not in('nmcd','msg','chip') then do;
				cell=month_&i.;
				last_mo=&i.;
				/*dual_cat=dual_&i.;
				mc_cat=mc_&i.;
				dis_cat=disabled_&i.;*/
			end;
		%end;
		
		/* create # of month for each cell variables */
		array cells{*} 3. cell_1-cell_17;
		do i=1 to 17;
			cells(i) = 0;
		end;
		array x{17} $ x1-x17 ("01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12" "13" "14" "15" "16" "17" );
		array months month_1-month_12;
		do i=1 to 17;
			do j=1 to 12;
				if months(j) = x(i) then cells(i)=cells(i)+1;
			end;
		end;
		
		/*create cell age categories*/
		if age_cat=1 then cell_age=catx('_','child',cell);
		else if age_cat=2 then cell_age=catx('_','adult',cell);
	   	else if age_cat=3 then cell_age=catx('_','senior',cell);
		else cell_age = '';

		/*these are how many months each category appears for each bene_id*/
		mo_mcd_only = cell_1 + cell_2 + cell_3 + cell_4 + cell_5 + cell_6 + cell_7 + cell_8;
		mo_dual = cell_9 + cell_10 + cell_11 + cell_12 + cell_13 + cell_14 + cell_15 + cell_16;
		mo_mc = cell_1 + cell_2 + cell_3 + cell_4 + cell_9 + cell_10 + cell_11 + cell_12;
		mo_ffs = cell_5 + cell_6 + cell_7 + cell_8 + cell_13 + cell_14 + cell_15 + cell_16;
		mo_dsbl = cell_1 + cell_2 + cell_5 + cell_6 + cell_9 + cell_10 + cell_13 + cell_14;
		mo_non_dsbl = cell_3 + cell_4 + cell_7 + cell_8 + cell_11 + cell_12 + cell_15 + cell_16;
		mo_ltss = cell_1 + cell_3 + cell_5 + cell_7 + cell_9 + cell_11 + cell_13 + cell_15;
		mo_non_ltss = cell_2 + cell_4 + cell_6 + cell_8 + cell_10 + cell_12 + cell_14 + cell_16;
		
		/*calculate number of distinct MAS/BOE categories a bene_id falls into (including nmcd, msg, and chip)*/
		do i=1 to 12;
	    	if not missing(months(i)) then do;
	      		if i lt 12 then do;
	        		if months(i) ~= months(i+1) then d_cell_n=sum(d_cell_n,1);
	      		end;
	      		else d_cell_n=sum(d_cell_n,1);
	   		 end;
	  	end;

		drop x1-x17 i j;

		if cell='01' then do; 				dual_cat=0; mc_cat=1; dis_cat=1; ltss_cat=1; foster_cat=0; end;
			else if cell='02' then do; 		dual_cat=0; mc_cat=1; dis_cat=1; ltss_cat=0; foster_cat=0; end;
			else if cell='03' then do; 		dual_cat=0; mc_cat=1; dis_cat=0; ltss_cat=1; foster_cat=0; end;
			else if cell='04' then do; 		dual_cat=0; mc_cat=1; dis_cat=0; ltss_cat=0; foster_cat=0; end;
			else if cell='05' then do; 		dual_cat=0; mc_cat=0; dis_cat=1; ltss_cat=1; foster_cat=0; end;
			else if cell='06' then do; 		dual_cat=0; mc_cat=0; dis_cat=1; ltss_cat=0; foster_cat=0; end;
			else if cell='07' then do; 		dual_cat=0; mc_cat=0; dis_cat=0; ltss_cat=1; foster_cat=0; end;
			else if cell='08' then do; 		dual_cat=0; mc_cat=0; dis_cat=0; ltss_cat=0; foster_cat=0; end;
			else if cell='09' then do; 		dual_cat=1; mc_cat=1; dis_cat=1; ltss_cat=1; foster_cat=0; end;
			else if cell='10' then do; 		dual_cat=1; mc_cat=1; dis_cat=1; ltss_cat=0; foster_cat=0; end;
			else if cell='11' then do;		dual_cat=1; mc_cat=1; dis_cat=0; ltss_cat=1; foster_cat=0; end;
			else if cell='12' then do;		dual_cat=1; mc_cat=1; dis_cat=0; ltss_cat=0; foster_cat=0; end;
			else if cell='13' then do;		dual_cat=1; mc_cat=0; dis_cat=1; ltss_cat=1; foster_cat=0; end;
			else if cell='14' then do;		dual_cat=1; mc_cat=0; dis_cat=1; ltss_cat=0; foster_cat=0; end;
			else if cell='15' then do;		dual_cat=1; mc_cat=0; dis_cat=0; ltss_cat=1; foster_cat=0; end;
			else if cell='16' then do;		dual_cat=1; mc_cat=0; dis_cat=0; ltss_cat=0; foster_cat=0; end;
			else if cell='17' then do;		dual_cat=9; mc_cat=9; dis_cat=9; ltss_cat=9; foster_cat=1; end;

		if not missing(cell) then do;
			if age_cat=1 then cell_age=catx('_','child',cell);
				else if age_cat=2 then cell_age=catx('_','adult',cell);
			   	else if age_cat=3 then cell_age=catx('_','senior',cell);
		end;
		
		cell_type1 = '';
		if missing(cell) then cell_type1 = 'msg';
		else if cell in ('01','02','03','04','06','07','08','09') then cell_type1 = "Medicaid Only";
		else if cell not in ('01','02','03','04','06','07','08','09','17') then cell_type1 = "Dual";
		else if cell = '17' then cell_type1 = 'Foster Care';
		
		cell_type2 = ' ';
		if missing(cell) then cell_type2 = 'msg';
		else if cell in ('01','02','03','04','09','10','11','12') then cell_type2 = "Managed Care";
		else if cell not in ('01','02','03','04','09','10','11','12','17') then cell_type2 = "Fee-For-Service";
		else if cell = '17' then cell_type2 = 'Foster Care';

		cell_type3 = '';
		if missing(cell) then cell_type3 = 'msg';
		else if cell in ('01','02','05','06','09','10','13','14') then cell_type3 = "Disability";
		else if cell not in ('01','02','05','06','09','10','13','14','17') then cell_type3 = "No Disability";
		else if cell = '17' then cell_type3 = 'Foster Care';

		cell_type4 = '';
		if missing(cell) then cell_type4 = 'msg';
		else if cell in ('01','03','05','07','09','11','13','15') then cell_type4 = "LTSS";
		else if cell not in ('01','03','05','07','09','11','13','15','17') then cell_type4 = "No LTSS";
		else if cell = '17' then cell_type4 = 'Foster Care';

		label 	
			_age = "Missing age flag"
			_chip_1 = "Missing or unknown chip_1 flag"
			_chip_2 = "Missing or unknown chip_2 flag"
			_chip_3 = "Missing or unknown chip_3 flag"
			_chip_4 = "Missing or unknown chip_4 flag"
			_chip_5 = "Missing or unknown chip_5 flag"
			_chip_6 = "Missing or unknown chip_6 flag"
			_chip_7 = "Missing or unknown chip_7 flag"
			_chip_8 = "Missing or unknown chip_8 flag"
			_chip_9 = "Missing or unknown chip_9 flag"
			_chip_10 = "Missing or unknown chip_10 flag"
			_chip_11 = "Missing or unknown chip_11 flag"
			_chip_12 = "Missing or unknown chip_12 flag"
			_died = "Missing or unknown died flag"
			_dual_1 = "Missing or unknown dual_1 flag"
			_dual_2 = "Missing or unknown dual_2 flag"
			_dual_3 = "Missing or unknown dual_3 flag"
			_dual_4 = "Missing or unknown dual_4 flag"
			_dual_5 = "Missing or unknown dual_5 flag"
			_dual_6 = "Missing or unknown dual_6 flag"
			_dual_7 = "Missing or unknown dual_7 flag"
			_dual_8 = "Missing or unknown dual_8 flag"
			_dual_9 = "Missing or unknown dual_9 flag"
			_dual_10 = "Missing or unknown dual_10 flag"
			_dual_11 = "Missing or unknown dual_11 flag"
			_dual_12 = "Missing or unknown dual_12 flag"
			_dual_full_1 = "Missing or unknown dual_full_1 flag"
			_dual_full_2 = "Missing or unknown dual_full_2 flag"
			_dual_full_3 = "Missing or unknown dual_full_3 flag"
			_dual_full_4 = "Missing or unknown dual_full_4 flag"
			_dual_full_5 = "Missing or unknown dual_full_5 flag"
			_dual_full_6 = "Missing or unknown dual_full_6 flag"
			_dual_full_7 = "Missing or unknown dual_full_7 flag"
			_dual_full_8 = "Missing or unknown dual_full_8 flag"
			_dual_full_9 = "Missing or unknown dual_full_9 flag"
			_dual_full_10 = "Missing or unknown dual_full_10 flag"
			_dual_full_11 = "Missing or unknown dual_full_11 flag"
			_dual_full_12 = "Missing or unknown dual_full_12 flag"
			_elig_1 = "Missing or unknown elig_1 flag"
			_elig_2 = "Missing or unknown elig_2 flag"
			_elig_3 = "Missing or unknown elig_3 flag"
			_elig_4 = "Missing or unknown elig_4 flag"
			_elig_5 = "Missing or unknown elig_5 flag"
			_elig_6 = "Missing or unknown elig_6 flag"
			_elig_7 = "Missing or unknown elig_7 flag"
			_elig_8 = "Missing or unknown elig_8 flag"
			_elig_9 = "Missing or unknown elig_9 flag"
			_elig_10 = "Missing or unknown elig_10 flag"
			_elig_11 = "Missing or unknown elig_11 flag"
			_elig_12 = "Missing or unknown elig_12 flag"
			_ltss = "Missing or unknown ltss flag"
			_male = "Missing or unknown male flag"
			_mc1 = "Missing or unknown mc1 flag"
			_mc2 = "Missing or unknown mc2 flag"
			_mc3 = "Missing or unknown mc3 flag"
			_mc4 = "Missing or unknown mc4 flag"
			_mc5 = "Missing or unknown mc5 flag"
			_mc6 = "Missing or unknown mc6 flag"
			_mc7 = "Missing or unknown mc7 flag"
			_mc8 = "Missing or unknown mc8 flag"
			_mc9 = "Missing or unknown mc9 flag"
			_mc10 = "Missing or unknown mc10 flag"
			_mc11 = "Missing or unknown mc11 flag"
			_mc12 = "Missing or unknown mc12 flag"
			_mcd_full_1 = "Missing or unknown mcd_full_1 flag"
			_mcd_full_2 = "Missing or unknown mcd_full_2 flag"
			_mcd_full_3 = "Missing or unknown mcd_full_3 flag"
			_mcd_full_4 = "Missing or unknown mcd_full_4 flag"
			_mcd_full_5 = "Missing or unknown mcd_full_5 flag"
			_mcd_full_6 = "Missing or unknown mcd_full_6 flag"
			_mcd_full_7 = "Missing or unknown mcd_full_7 flag"
			_mcd_full_8 = "Missing or unknown mcd_full_8 flag"
			_mcd_full_9 = "Missing or unknown mcd_full_9 flag"
			_mcd_full_10 = "Missing or unknown mcd_full_10 flag"
			_mcd_full_11 = "Missing or unknown mcd_full_11 flag"
			_mcd_full_12 = "Missing or unknown mcd_full_12 flag"
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
			cash_1 = "Cash assistance - Jan"
			cash_2 = "Cash assistance - Feb"
			cash_3 = "Cash assistance - Mar"
			cash_4 = "Cash assistance - Apr"
			cash_5 = "Cash assistance - May"
			cash_6 = "Cash assistance - Jun"
			cash_7 = "Cash assistance - Jul"
			cash_8 = "Cash assistance - Aug"
			cash_9 = "Cash assistance - Sep"
			cash_10 = "Cash assistance - Oct"
			cash_11 = "Cash assistance - Nov"
			cash_12 = "Cash assistance - Dec"
			cell_n = "Flag for sums"
			cell = "Latest known MAS/BOE/Foster category"
			cell_1="cell_1 flag"
			cell_2="cell_2 flag"
			cell_3="cell_3 flag"
			cell_4="cell_4 flag"
			cell_5="cell_5 flag"
			cell_6="cell_6 flag"
			cell_7="cell_7 flag"
			cell_8="cell_8 flag"
			cell_9="cell_9 flag"
			cell_10="cell_10 flag"
			cell_11="cell_11 flag"
			cell_12="cell_12 flag"
			cell_13="cell_13 flag"
			cell_14="cell_14 flag"
			cell_15="cell_15 flag"
			cell_16="cell_16 flag"
			cell_17="cell_17 flag"
			cell_age = "Cell and age category"
			cell_type1 = "Medicaid only or dual"
			cell_type2 = "Managed care or FFS"
			cell_type3 = "Disability or no disability"
			cell_type4 = "LTSS or no LTSS"
			chip_1 = "CHIP - Jan"
			chip_2 = "CHIP - Feb"
			chip_3 = "CHIP - Mar"
			chip_4 = "CHIP - Apr"
			chip_5 = "CHIP - May"
			chip_6 = "CHIP - Jun"
			chip_7 = "CHIP - Jul"
			chip_8 = "CHIP - Aug"
			chip_9 = "CHIP - Sep"
			chip_10 = "CHIP - Oct"
			chip_11 = "CHIP - Nov"
			chip_12 = "CHIP - Dec"
			cltc = "Sum of CLTC_FFS_PYMT_AMT_11-CLTC_FFS_PYMT_AMT_40"
			d_cell_n ="Number of Unique Statuses"
			died_n ="Number Dying in Year"
			dis_cat ="Disability Category"
			disabled_1 = "Disabled - Jan"
			disabled_2 = "Disabled - Feb"
			disabled_3 = "Disabled - Mar"
			disabled_4 = "Disabled - Apr"
			disabled_5 = "Disabled - May"
			disabled_6 = "Disabled - Jun"
			disabled_7 = "Disabled - Jul"
			disabled_8 = "Disabled - Aug"
			disabled_9 = "Disabled - Sep"
			disabled_10 = "Disabled - Oct"
			disabled_11 = "Disabled - Nov"
			disabled_12 = "Disabled - Dec"
			dual_1 = "Dual eligible - Jan"
			dual_2 = "Dual eligible - Feb"
			dual_3 = "Dual eligible - Mar"
			dual_4 = "Dual eligible - Apr"
			dual_5 = "Dual eligible - May"
			dual_6 = "Dual eligible - Jun"
			dual_7 = "Dual eligible - Jul"
			dual_8 = "Dual eligible - Aug"
			dual_9 = "Dual eligible - Sep"
			dual_10 = "Dual eligible - Oct"
			dual_11 = "Dual eligible - Nov"
			dual_12 = "Dual eligible - Dec"
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
			mc_1 = "Managed care - Jan"
			mc_2 = "Managed care - Feb"
			mc_3 = "Managed care - Mar"
			mc_4 = "Managed care - Apr"
			mc_5 = "Managed care - May"
			mc_6 = "Managed care - Jun"
			mc_7 = "Managed care - Jul"
			mc_8 = "Managed care - Aug"
			mc_9 = "Managed care - Sep"
			mc_10 = "Managed care - Oct"
			mc_11 = "Managed care - Nov"
			mc_12 = "Managed care - Dec"
			mc_cat ="Managed Care Category"
			mcd_full_1 = "Eligible for full Medicaid benefits - Jan"
			mcd_full_2 = "Eligible for full Medicaid benefits - Feb"
			mcd_full_3 = "Eligible for full Medicaid benefits - Mar"
			mcd_full_4 = "Eligible for full Medicaid benefits - Apr"
			mcd_full_5 = "Eligible for full Medicaid benefits - May"
			mcd_full_6 = "Eligible for full Medicaid benefits - Jun"
			mcd_full_7 = "Eligible for full Medicaid benefits - Jul"
			mcd_full_8 = "Eligible for full Medicaid benefits - Aug"
			mcd_full_9 = "Eligible for full Medicaid benefits - Sep"
			mcd_full_10 = "Eligible for full Medicaid benefits - Oct"
			mcd_full_11 = "Eligible for full Medicaid benefits - Nov"
			mcd_full_12 = "Eligible for full Medicaid benefits - Dec"
			mo_dsbl ="Months eligible for disability'"
			mo_dual ="Months eligible for dual'"
			mo_ffs ="Months eligible for FFS'"
			mo_ltss ="Months eligible for long-term support services"
			mo_mc ="Months eligible for managed care'"
			mo_mcd_only ="Months eligible for Medicaid only"
			mo_non_dsbl ="Months eligible for non-disability services'"
			mo_non_ltss ="Months eligible for non-long term support services'"
			pm_n = "Eligible months"
			st_cnty = "State abbreviation and county code"
			state_cd = "State abbreviation"
			tot_pay ="TOT_MDCD_PYMT_AMT "
			year = "Claims year"
			recipno = "Recipient number for linking CDPS data"
			;
	run;

%mend cells;

%cells(indata=data.maxdata_ps_2012 ,outdata=personlevel);


%macro geo_fix(indata=,outdata=);
ods excel file="&report_folder.\geofix_summary_&fname..xlsx";
ods excel options(sheet_name="geo fix summary" sheet_interval="none");

proc sql;
	create table max_zip_merge as
	select a.zip_code, a.st_cnty,b.*, b.st_cnty as max_st_cnty,
		(a.zip_code = .) as no_zip_match,
		(a.st_cnty = ' ') as no_cnty_match,
		(b.zip_miss = a.zip_code) as zip_match,
		(b.st_cnty = a.st_cnty) as cnty_match,
		case 
			when b.zip_miss in (select distinct zip_code from area.all_zips) then 1
			when b.zip_miss = . then .
			else 0
		end as valid_zip, /*1 = MAX zip in all_zips data, . = MAX zip is missing, 0 = MAX zip is not in all_zips data*/
		case 
			when b.st_cnty in (select distinct st_cnty from area.all_zips) then 1
			when b.st_cnty = ' ' then .
			else 0
		end as valid_cnty /*1 = MAX county in all_zips data, . = MAX county is missing, 0 = MAX county is not in all_zips data*/
	from area.all_zips a full join &indata. b
	on a.zip_code = b.zip_miss and a.st_cnty = b.st_cnty /*need to match on both bc these either var alone many-to-many match*/
	where not missing(b.st_cnty) or not missing(b.zip_miss);
quit;

proc odstext;
  p "Max and Zip Data Merge Results";
  p "first two cols: 0 = match between MAX and Zips, 1 = no match between MAX and Zips";
  p "second two cols: 0 = MAX var not in Zips data, 1 = MAX var in Zips data, . = MAX var is missing";
run;

proc freq data=max_zip_merge;
	title "Max and Zip Data Merge Results";
	title2 "first two cols: 0 = match between MAX and Zips, 1 = no match between MAX and Zips";
	title3 "second two cols: 0 = MAX var not in Zips data, 1 = MAX var in Zips data, . = MAX var is missing";
	tables no_zip_match*no_cnty_match*valid_zip*valid_cnty/list missing nopercent;
run;

/**************************************/
/*Start work on problem zips/counties*/
/**************************************/
proc sql;
	create table invalid_zip as 
	select *
	from max_zip_merge
	where (valid_zip in (0, .) /*invalid or missing zips*/ and valid_cnty = 1 /*with valid counties*/) or 
			(valid_zip = 1 and valid_cnty = 1 and no_zip_match = 1 and no_cnty_match = 1) /*invalid zip/county combos - judgement call here to replace the zips for these obs*/
	order by max_st_cnty;
quit;

proc sql;
	create table validzip_validcnty as
	select * 
	from max_zip_merge
	where valid_zip = 1 and valid_cnty = 1 and no_zip_match = 0 and no_cnty_match = 0
	order by zip_miss;
quit;

proc sql;
	create table validzip_invalidcnty as
	select * 
	from max_zip_merge
	where valid_zip = 1 and valid_cnty in (0,.)
	order by zip_miss;
quit;

/**************************************/
/*Fix invalid zips with valid counties*/
/**************************************/
proc sql; /*need a data set with all possible strata only*/
	create table invalidzip_validcnty_df as
	select st_cnty, zip_code
	from area.all_zips
	where st_cnty in (select distinct max_st_cnty from invalid_zip)
	order by st_cnty;
quit;

proc sql;
	create table zip_strata as
	select max_st_cnty as st_cnty, count(max_st_cnty) as _nsize_
	from invalid_zip
	group by max_st_cnty;
quit;

/*select random zip codes based on county*/
/*this will generate notes because sample size is greater than sampling unit - that is necessary for our needs*/
proc surveyselect data=invalidzip_validcnty_df noprint 
      method=urs 
      n=zip_strata /*data set containing stratum sample sizes*/
      seed=1953
      out=random_zips;
   strata st_cnty;
run;

*reorganize file;
data zip_long;
	set random_zips; /*de-flatten random_zips*/
	do i = 1 to NumberHits;
	   output;
	end;
run;

proc sort data=zip_long (keep=st_cnty zip_code);
	by st_cnty;
run;

data invalid_zip_replaced;
	merge invalid_zip zip_long;
run;

/********************************/
/*Fix zips with invalid counties*/
/********************************/
proc sql; /*need a data set with all possible strata only*/
	create table validzip_invalidcnty_df as
	select st_cnty, zip_code
	from area.all_zips
	where zip_code in (select distinct zip_miss from validzip_invalidcnty)
	order by zip_code;
quit;

proc sql;
	create table cnty_strata as
	select zip_miss as zip_code, count(zip_miss) as _nsize_
	from validzip_invalidcnty
	group by zip_miss;
quit;

/*select random zip codes based on county*/
/*this will generate notes because sample size is greater than sampling unit - that is necessary for our needs*/
proc surveyselect data=validzip_invalidcnty_df noprint 
      method=urs 
      n=cnty_strata /*data set containing stratum sample sizes*/
      seed=1953
      out=random_cnty;
   strata zip_code;
run;

*reorganize file;
data cnty_long;
	set random_cnty; /*de-flatten random_zips*/
	do i = 1 to NumberHits;
	   output;
	end;
run;

proc sort data=cnty_long (keep=st_cnty zip_code);
	by zip_code;
run;

proc odstext;
  p "After Invalid Zip Fixes";
run;

data invalid_cnty_replaced;
	merge validzip_invalidcnty cnty_long;
run;
/*************************************/
/*merge new zip file with valid zips;*/
/*************************************/
data max_zip_complete;
	set invalid_zip_replaced invalid_cnty_replaced validzip_validcnty;
run;

proc odstext;
  p "Zip Fix Summary";
run;

proc sql;
	create table &outdata. as
	select *,
		case
			when valid_zip in (0,.) or (no_zip_match = 1 and no_cnty_match = 1) then zip_code
			when valid_zip = 1 then zip_miss
			else .
		end as zip_fx label="Zip code",
		case
			when valid_zip in (0,.) or (no_zip_match = 1 and no_cnty_match = 1) then 1
			when valid_zip = 1 then 0
			else .
		end as replaced_zip label="1 if zip was replaced during geo fixes",
		case
			when valid_cnty in (0,.)then st_cnty
			when valid_cnty = 1 then max_st_cnty
			else ' '
		end as cnty_fx format=$8. label="State abbreviation and county code",
		case
			when valid_cnty in (0,.)then 1
			when valid_cnty = 1 then 0
			else .
		end as replaced_cnty label="1 if state/county was replaced during geo fixes"
	from max_zip_complete;

	create table check_zip_fx as
	select replaced_zip, replaced_cnty, no_zip_match, no_cnty_match,valid_zip,valid_cnty,
		(b.zip_fx = a.zip_code) as zip_match, 
		(b.cnty_fx = a.st_cnty) as cnty_match
	from area.all_zips a right join &outdata. b
	on a.zip_code = b.zip_fx and a.st_cnty = b.cnty_fx
	where not missing(b.cnty_fx) or not missing(b.zip_fx);
quit;
proc odstext;
  p "Max and Zip Data Merge Results";
run;

proc freq data=check_zip_fx;
	title "Max and Zip Data Merge Results";
	tables zip_match*cnty_match*replaced_zip*replaced_cnty*valid_zip*valid_cnty/list missing;
run;
title;

proc freq data=&outdata.;
	title "Zip and County Code Fix Frequencies";
	tables zip_fx cnty_fx;
	format zip_fx missing_zip. cnty_fx $missing_char.; 
run;
title;
ods excel close;
%mend;
%geo_fix(indata=personlevel, outdata=personlevel_geofixed);

/***********************/
/*Send data to perm lib*/
/***********************/

proc sql;
	create table space.id_pop_&space_name. as
	select *
	from personlevel_geofixed (drop = county_miss cnty_match no_cnty_match no_zip_match valid_cnty valid_zip zip_code zip_match zip_miss max_st_cnty st_cnty rename=(cnty_fx = county zip_fx=zipcode))
	where not missing(age_cat) and not missing(dis_cat) and not missing (dual_cat) and not missing (mc_cat) and not missing (ltss_cat);
quit;

proc sql;
	create table space.id_pop_dropped_&space_name. as
	select *
	from personlevel_geofixed (drop = no_cnty_match no_zip_match valid_cnty valid_zip zip_code zip_match zip_miss rename = (cnty_fx = county zip_fx=zipcode))
	where missing(age_cat) or missing(dis_cat) or missing (dual_cat) or missing (mc_cat) or missing (ltss_cat);
quit;

ods excel file="&report_folder.\dropped_summary_&fname..xlsx";
ods excel options(sheet_name="dropped summary" sheet_interval="none");

proc sql;
	select count(*) as num_bad_geo_dropped
	from personlevel a left join personlevel_geofixed b
	on a.bene_id = b.bene_id
	where missing(b.bene_id);
quit;

proc freq data=space.id_pop_dropped_&space_name.;
	tables age_cat*dual_cat*mc_cat*dis_cat*ltss_cat/list missing;
run;

ods excel close; 
/*create finder file
data space.finder_file_&space_name.;
	set space.id_pop_&space_name. (keep = bene_id cell_:); 
run;
*/
proc printto;run;
