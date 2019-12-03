/* 
   Create a dataset with desired MAX variables for given cells at the  county and national level,
    based on latest eligible month, and merge with AHRF county-level data

   Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)

*/

* Date for version control;
%let fname = %sysfunc(date(),date9.)_t%sysfunc(compress(%sysfunc(time(),time8.),:.));
options obs=100000;
* log;
*PROC PRINTTO PRINT="P:\MCD-SPVR\log\02_max_ahrf_&sysdate..lst"
               LOG="P:\MCD-SPVR\log\02_max_ahrf_&sysdate..log" NEW;
*RUN;

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
	/*limit incoming data using proc sql for speed*/
	proc sql;
		create table limit_incoming_dat as
		select *
		from &indata. (keep = bene_id state_cd
			EL_RSDNC_ZIP_CD_LTST EL_RSDNC_CNTY_CD_LTST EL_DOB MC_COMBO_MO:
			EL_MDCR_DUAL_MO_: EL_RSTRCT_BNFT_FLG: MAX_ELG_CD_MO: EL_CHIP_FLAG: 
			EL_SEX_CD TOT_MDCD_PYMT_AMT EL_ELGBLTY_MO_CNT EL_MAX_ELGBLTY_CD_LTST
			EL_DOD MDCR_DOD CLTC: FEE_FOR_SRVC_IND_07 TOT_MDCD_PYMT_AMT)
		where not missing(bene_id);
	quit;

	/* create person-level dataset with appropriate variables and cell types */
	data &outdata.;
		format cell_type1-cell_type4 $16. cell $15. county_miss $3. zip_miss 5.; 
		set limit_incoming_dat;
	  	/****************************/
		/*Variable creation/recoding*/
		/****************************/
		year = 2012;
		cell_n = 1;
		pm_n = EL_ELGBLTY_MO_CNT;
		tot_pay = TOT_MDCD_PYMT_AMT;     /*could also add other TOS-specific payment amounts here */

		/* LTSS */
		cltc = sum(of CLTC_FFS_PYMT_AMT_11--CLTC_FFS_PYMT_AMT_40);

		if Fee_for_srvc_ind_07=1 then lt_nurs=1;
		else lt_nurs=0;
		if missing(Fee_for_srvc_ind_07) then _ltss=1;

		if cltc>0 or lt_nurs=1 then ltss=1;
		else ltss=0;

		/* calculate age */
		calcdate = mdy(12,1,2012);
		format calcdate mmddyy8.;
		age = int(intck('MONTH',EL_DOB,calcdate)/12);
		if month(EL_DOB) = month(calcdate) 
			then age = age-(day(EL_DOB)>day(calcdate));	

		if age>=0 and age<=18 then age_cat=1;
		else if age>=19 and age<=64 then age_cat=2;
		else if age>=65 then age_cat=3;

		if age=0 then age_0=1;
		if age>=1 and age<=5 then age_1_5=1;
		if age>=6 and age<=18 then age_6_18=1;
		if age>=19 and age<=44 then age_19_44=1;
		if age>=45 and age<=64 then age_45_64=1;
		if age>=65 and age <=84 then age_65_84=1;
		if age>=85 then age_85p=1;

		if age_cat=1 then cell_age=catx('_','child',cell);
		else if age_cat=2 then cell_age=catx('_','adult',cell);
	   	else if age_cat=3 then cell_age=catx('_','senior',cell);

		/* recode gender */
		if el_sex_cd='M' then male=1;
		else if el_sex_cd='F' then male=0;

		/* mas */
		if substr(EL_MAX_ELGBLTY_CD_LTST,1,1)="1" then mas_cash=1;
		else mas_cash=0;

		if substr(EL_MAX_ELGBLTY_CD_LTST,1,1)="2" then mas_mn=1;
		else mas_mn=0;

		if substr(EL_MAX_ELGBLTY_CD_LTST,1,1)="3" then mas_pov=1;
		else mas_pov=0;

		if substr(EL_MAX_ELGBLTY_CD_LTST,1,1)="4" and MAX_ELG_CD_MO_12~="48" then mas_oth=1;
		else mas_oth=0;

		if substr(EL_MAX_ELGBLTY_CD_LTST,1,1)="5" then mas_1115=1;
		else mas_1115=0;

		/* boe */
		if substr(EL_MAX_ELGBLTY_CD_LTST,2,1)="1" then boe_aged=1;
		else boe_aged=0;

		if substr(EL_MAX_ELGBLTY_CD_LTST,2,1)="2" then boe_disabled=1;
		else boe_disabled=0;

		if substr(EL_MAX_ELGBLTY_CD_LTST,2,1)="4" then boe_child=1;
		else boe_child=0;

		if substr(EL_MAX_ELGBLTY_CD_LTST,2,1)="5" then boe_adult=1;
		else boe_adult=0;

		if substr(EL_MAX_ELGBLTY_CD_LTST,2,1)="6" then boe_uchild=1;
		else boe_uchild=0;

		if substr(EL_MAX_ELGBLTY_CD_LTST,2,1)="7" then boe_uadult=1;
		else boe_uadult=0;

		if substr(EL_MAX_ELGBLTY_CD_LTST,2,1)="8" then boe_fchild=1;
		else boe_fchild=0;

		/* disability status & cash assistance (eligibility) */	
		if EL_MAX_ELGBLTY_CD_LTST in ("12","22","32","42","52") then disabled_12=1;
		else if EL_MAX_ELGBLTY_CD_LTST~="99" and not missing(EL_MAX_ELGBLTY_CD_LTST) then disabled_12=0;


		if EL_MAX_ELGBLTY_CD_LTST in ("11","12","14","15","16","17") then cash_12=1;
		else if EL_MAX_ELGBLTY_CD_LTST~="99" and not missing(EL_MAX_ELGBLTY_CD_LTST) then cash_12=0;
		
		if missing(EL_MAX_ELGBLTY_CD_LTST) or EL_MAX_ELGBLTY_CD_LTST="99" then _elig_12=1;
		else _elig_12=0;

		/* CHIP */
		if EL_CHIP_FLAG_12 in(2,3)  then chip_12=1;
		else if EL_CHIP_FLAG_12 ~=9 and not missing(EL_CHIP_FLAG_12)  then chip_12=0;

		if EL_CHIP_FLAG_12=9 or missing(EL_CHIP_FLAG_12) then _chip_12=1;
		else _chip_12=0;

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


			/*restricted benefits*/
			if EL_RSTRCT_BNFT_FLG_&i. = "1" or EL_MDCR_DUAL_MO_&i. in ("02","04","08","52","54","58") then mcd_full_&i.=1;
			else if EL_RSTRCT_BNFT_FLG_&i. ~="9" and not missing(EL_RSTRCT_BNFT_FLG_&i.) then mcd_full_&i.=0;

			if missing(EL_RSTRCT_BNFT_FLG_&i.) or EL_RSTRCT_BNFT_FLG_&i.="9" then _mcd_full_&i.=1;
			else _mcd_full_&i.=0;


			/* disability status & cash assistance (eligibility) */	
			if MAX_ELG_CD_MO_&i. in ("12","22","32","42","52") then disabled_&i.=1;
			else if MAX_ELG_CD_MO_&i.~="99" and not missing(MAX_ELG_CD_MO_&i.) then disabled_&i.=0;


			if MAX_ELG_CD_MO_&i. in ("11","12","14","15","16","17") then cash_&i.=1;
			else if MAX_ELG_CD_MO_&i.~="99" and not missing(MAX_ELG_CD_MO_&i.) then cash_&i.=0;
			
			if missing(MAX_ELG_CD_MO_&i.) or MAX_ELG_CD_MO_&i.="99" then _elig_&i.=1;
			else _elig_&i.=0;

			/* CHIP */
			if EL_CHIP_FLAG_&i. in(2,3)  then chip_&i.=1;
			else if EL_CHIP_FLAG_&i. ~=9 and not missing(EL_CHIP_FLAG_&i.)  then chip_&i.=0;

			if EL_CHIP_FLAG_&i.=9 or missing(EL_CHIP_FLAG_&i.) then _chip_&i.=1;
			else _chip_&i.=0;

			/* create cells */
			if chip_&i.=1 then month_&i.="chip";
			else if MAX_ELG_CD_MO_&i.="00" then month_&i.="nmcd"; /*not medicaid*/
			else if _mc&i=1 or _dual_&i.=1 or (_dual_full_&i.=1 and _mcd_full_&i.=1) or _elig_&i.=1 or _chip_&i.=1 or _ltss=1 then month_&i.="msg"; /*missing*/
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
		%end;

		/*remove inelibile records & 
		find latest month_[month] with data*/
		%do i=1 %to 12;
			/*proc sql noprint;
				select count(*) into :num_dropped_&i.
				from person_level
				where month_&i. in ("nmcd","msg","chip");
			quit; */
			if month_&i. not in('nmcd','msg','chip') then do;
				cell=month_&i.;
				last_mo=&i.;
				dual_cat=dual_&i.;
				mc_cat=mc_&i.;
				dis_cat=disabled_&i.;
			end;
		%end;
		
		/* create # of month variables */
		array cells{*} 3. cell_1-cell_17;
		do i=1 to 17;
			cells(i) = 0;
		end;
		array x{17} $ x1-x17 ("1" "2" "3" "4" "5" "6" "7" "8" "9" "10" "11" "12" "13" "14" "15" "16" "17" );
		array months month_1-month_12;
		do i=1 to 17;
			do j=1 to 12;
				if months(j) = x(i) then cells(i)=cells(i)+1;
			end;
		end;

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

		if cell='1' then do; 				cell='01'; dual_cat=0; mc_cat=1; dis_cat=1; ltss_cat=1; foster_cat=0; end;
			else if cell='2' then do; 		cell='02'; dual_cat=0; mc_cat=1; dis_cat=1; ltss_cat=0; foster_cat=0; end;
			else if cell='3' then do; 		cell='03'; dual_cat=0; mc_cat=1; dis_cat=0; ltss_cat=1; foster_cat=0; end;
			else if cell='4' then do; 		cell='04'; dual_cat=0; mc_cat=1; dis_cat=0; ltss_cat=0; foster_cat=0; end;
			else if cell='5' then do; 		cell='05'; dual_cat=0; mc_cat=0; dis_cat=1; ltss_cat=1; foster_cat=0; end;
			else if cell='6' then do; 		cell='06'; dual_cat=0; mc_cat=0; dis_cat=1; ltss_cat=0; foster_cat=0; end;
			else if cell='7' then do; 		cell='07'; dual_cat=0; mc_cat=0; dis_cat=0; ltss_cat=1; foster_cat=0; end;
			else if cell='8' then do; 		cell='08'; dual_cat=0; mc_cat=0; dis_cat=0; ltss_cat=0; foster_cat=0; end;
			else if cell='9' then do; 		cell='09'; dual_cat=1; mc_cat=1; dis_cat=1; ltss_cat=1; foster_cat=0; end;
			else if cell='10' then do; 		dual_cat=1; mc_cat=1; dis_cat=1; ltss_cat=0; foster_cat=0; end;
			else if cell='11' then do;		dual_cat=1; mc_cat=1; dis_cat=0; ltss_cat=1; foster_cat=0; end;
			else if cell='12' then do;		dual_cat=1; mc_cat=1; dis_cat=0; ltss_cat=0; foster_cat=0; end;
			else if cell='13' then do;		dual_cat=1; mc_cat=0; dis_cat=1; ltss_cat=1; foster_cat=0; end;
			else if cell='14' then do;		dual_cat=1; mc_cat=0; dis_cat=1; ltss_cat=0; foster_cat=0; end;
			else if cell='15' then do;		dual_cat=1; mc_cat=0; dis_cat=0; ltss_cat=1; foster_cat=0; end;
			else if cell='16' then do;		dual_cat=1; mc_cat=0; dis_cat=0; ltss_cat=0; foster_cat=0; end;
			else if cell='17' then do;		age_cat=1; dual_cat=9; mc_cat=9; dis_cat=9; ltss_cat=9; foster_cat=1; 
		end;
		if not missing(cell) then do;
			if age_cat=1 then cell_age=catx('_','child',cell);
				else if age_cat=2 then cell_age=catx('_','adult',cell);
			   	else if age_cat=3 then cell_age=catx('_','senior',cell);
		end;
		
		cell_type1 = '';
		if missing(cell) then cell_type1 = 'msg';
		else if cell in ('1','2','3','4','6','7','8','9') then cell_type1 = "Medicaid Only";
		else if cell not in ('1','2','3','4','6','7','8','9','17') then cell_type1 = "Dual";
		else if cell = '17' then cell_type1 = 'Foster Care';
		
		cell_type2 = ' ';
		if missing(cell) then cell_type2 = 'msg';
		else if cell in ('1','10') then cell_type2 = "Managed Care";
		else if cell not in ('1','10','17') then cell_type2 = "Fee-For-Service";
		else if cell = '17' then cell_type2 = 'Foster Care';

		cell_type3 = '';
		if missing(cell) then cell_type3 = 'msg';
		else if cell in ('1','2','5','6','9','10/','13','14') then cell_type3 = "Disability";
		else if cell not in ('1','2','5','6','9','10','13','14','17') then cell_type3 = "No Disability";
		else if cell = '17' then cell_type3 = 'Foster Care';

		cell_type4 = '';
		if missing(cell) then cell_type4 = 'msg';
		else if cell in ('1','3','5','7','9','11','13','15') then cell_type4 = "LTSS";
		else if cell not in ('1','3','5','7','9','11','13','15','17') then cell_type4 = "No LTSS";
		else if cell = '17' then cell_type4 = 'Foster Care';
	run;

%mend cells;

%cells(indata=data.maxdata_ps_2012 ,outdata=personlevel);

options obs = MAX;
proc sql;
	create table max_zip_merge as
	select *, b.STATE_CD as max_state, b.st_cnty as max_st_cnty, b.zip_miss,
		(b.zip_miss ne .) as max_zip_ind,
		(b.st_cnty ne ' ') as cnty_zip_ind,
		(b.zip_miss = a.zip_code) as zip_match,
		(b.st_cnty = a.st_cnty) as cnty_match,
		case 
			when b.zip_miss in (select distinct zip_code from area.all_zips) then 1
			when b.zip_miss = . then .
			else 0
		end as valid_zip,
		case 
			when b.st_cnty in (select distinct st_cnty from area.all_zips) then 1
			when b.st_cnty = ' ' then .
			else 0
		end as valid_cnty
	from area.all_zips a right join personlevel b
	on a.zip_code = b.zip_miss and a.st_cnty = b.st_cnty
	where not missing(b.st_cnty) or not missing(b.zip_miss);
quit;

proc freq data=max_zip_merge;
	title "Max and Zip Data Merge Results";
	tables max_zip_ind*cnty_zip_ind*valid_zip*valid_cnty/list missing nopercent;
run;

proc sql;
	create table invalid_zip as
	select * 
	from max_zip_merge
	where valid_zip in (0, .) and not missing(max_st_cnty) and max_st_cnty in (select distinct st_cnty from area.all_zips)
	order by max_st_cnty;
quit;

proc sql;
	create table valid_zip as
	select * 
	from max_zip_merge
	where valid_zip = 1
	order by zip_code;
quit;

proc sql; /*need a data set with all possible strata only*/
	create table invalid_zip_cntygood as
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

*select random zip codes based on county;
proc surveyselect data=invalid_zip_cntygood noprint 
      method=urs 
      n=zip_strata /*data set containing stratum sample sizes*/
      seed=1953
      out=random_zips;
   strata st_cnty;
run;

*reorganize file;
data zip_long;
	set random_zips;
	do i = 1 to NumberHits;
	   output;
	end;
run;

proc sort data=zip_long;
	by st_cnty;
run;

proc sql noprint;
	select zip_code into :zip_code_replace
	from zip_long;

	update invalid_zip
	set zip_code = &zip_code_replace.;
quit;
	
*merge new zip file with valid zips;
data max_zip_complete;
	set invalid_zip valid_zip;
run;


















proc sql;
	create table drop_missing_geodata as 
	select *, case
			when not missing(STATE_CD) and not missing(county_miss) and not missing(zip_miss) then catx('-',STATE_CD,county_miss,zip_miss) 
			else ' '
			end as max_geo,
			 case
			when not missing(STATE_CD) and not missing(county_miss) then catx('-',STATE_CD,county_miss) 
			else ' '
			end as state_county
	from personlevel
	where (not missing(STATE_CD) and not missing(county_miss)) or not missing(zip_miss);
	
	title All PersonLevel Data Count;
	select count(*) label "All PersonLevel Data Count"
	from personlevel;
	
	title Without Obs Missing ALL Geodata Count;
	select count(*) label "Obs with Zip or State/County or Both Count"
	from drop_missing_geodata;
	title;
quit;

proc sql;
	create table match_geos as
	select *,a.bene_id, a.max_geo, b.zips_geo, a.zip_miss, b.zip_code, a.state_county, b.st_cnty,
		(a.max_geo = b.zips_geo) as both_indic,
		(a.max_geo ne b.zips_geo) as max_only_indic
	from drop_missing_geodata a left join zips b
	on a.max_geo = b.zips_geo;
quit;

proc freq data=match_geos;
	title "Geo Matches in Max & Zips data";
	tables both_indic*in_max_only/list;
run;

proc sql;
	create table unmatchgeos_vzip_vcnty as
	select *, 
		case when zip_miss in ( /*look for whether or not the zip is present at all in the zips data*/
			select distinct(zip_code)
				from zips) then 1 
			else 0 
		end as valid_zip, 
		case when state_county in ( /*look for whether or not the county is present in the zips data*/
			select distinct(st_cnty)
				from zips) then 1 
			else 0 
		end as valid_county
	from match_geos
	where both_indic = 0;
quit;
/*Below is looking at the invalid combinations of valid zips and counties. From this, I decided county is more reliable than zip
proc sql;
	create table valid_geos_invalidcombo as
	select distinct(a.state_county),a.zip_miss, b.zip_code as zips_zip, b.st_cnty as zips_county
	from unmatchgeos_vzip_vcnty a left join zips b
	on a.state_county = b.st_cnty
	where a.valid_zip = 1 and a.valid_county=1
	;
quit;

proc print data=valid_geos_invalidcombo;run;
*/
proc freq data=unmatchgeos_vzip_vcnty;
	title "Unmatched geos with Valid/Invalid Zips and Counties";
	title2 "valid = present in zips data set";
	tables valid_zip*valid_county/list nopercent;
run;

ods excel file="C:\Users\LDURBAK\Documents\geo_matching_fixes_&fname..xlsx";
ods excel options(sheet_name="mismatches" sheet_interval="none");

%macro char_random_assignment(valid_variable,invalid_variable, match_variable,indata,condition); /*valid_variable is char*/
	proc odstext;
	  p " Number of Obs with Valid &valid_variable.";
	run;
%let valid_variable=state_county;
%let invalid_variable = zip_code;
%let match_variable = st_cnty;
%let indata=unmatchgeos_vzip_vcnty;
%let condition=(valid_zip = 0 and valid_county = 1) or (valid_zip = 1 and valid_county = 1);


	proc sql;
		/*identify invalid zip codes from MAX data with valid state/county data*/
		create table data_to_fix_&valid_variable. as
		select bene_id, &valid_variable., &match_variable., &invalid_variable.
		from &indata.
		where &condition.;

		title Number of Obs with Valid &valid_variable.;
		select count(*) as num_valid&valid_variable.
		from data_to_fix_&valid_variable.;
		title;

		/*figure out how many of which st_cnty combos need valid zip codes*/
		create table sample_info as
		select distinct(&valid_variable.) as var_lookup, count(&valid_variable.) as _nsize_
		from data_to_fix_&valid_variable.
		group by &valid_variable.;
	quit;

	proc sql;
		create table zip_strata as
		select state_county as st_cnty, count(state_county) as _nsize_
		from data_to_fix_&valid_variable.
		group by state_county;
	quit;

		/*create random sample of N size (using N from sample_wrownum) of zips (with replacement) using all possible zips the st_cnty*/
		proc surveyselect data=zips
			  outhits noprint
			  method=urs 
		      n=zip_strata
		      seed=31
		      out=random_samp /*(keep=&match_variable. &invalid_variable)*/;
		   strata &match_variable.;
		run;

		proc sql noprint;
			/*replace the missing zips with the random zips*/
			select &match_variable., &invalid_variable. into :var, :replacement
			from random_samp_&i.;
			
			update data_to_fix_&valid_variable.
			set &invalid_variable. = &replacement.
			where &valid_variable. = "&var." and missing(&match_variable.);
		quit;
	%end;
	
	/*delete subsetted zip data*/
	proc datasets library=work nolist;
		delete random_samp_:;
	quit;
















































	proc sql;
		/*identify invalid zip codes from MAX data with valid state/county data*/
		create table data_to_fix_&valid_variable. as
		select bene_id,state_county, &valid_variable., &match_variable., &invalid_variable.
		from &indata.
		where &condition.;

		title Number of Obs with Valid &valid_variable.;
		select count(*) as num_valid&valid_variable.
		from data_to_fix_&valid_variable.;
		title;

		/*figure out how many of which st_cnty combos need valid zip codes*/
		create table sample as
		select distinct(&valid_variable.) as var_lookup, count(&valid_variable.) as num_assignments_needed
		from data_to_fix_&valid_variable.
		group by &valid_variable.;

		/*add row num and tot_rows for looping*/
		create table sample_wrownum as
		select *, monotonic() as row_num
		from sample;
		
		reset noprint;
		select count(*) into :tot_rows
		from sample_wrownum;
	quit;

	%do i=1 %to &tot_rows.;
		proc sql noprint;
			/*create one subset per distinct st_cnty with invalid zip data using info in sample_wrownum table*/
			select var_lookup, num_assignments_needed into :var, :num /*var = valid zip code, num = the number of random st_cnty needed*/
			from sample_wrownum
			where row_num = &i.;

			create table zip_sub_&i. as /*this table has all the st_cnty associated with that &var zip code*/
			select *
			from zips
			where &match_variable. = "&var.";
		quit;

		/*create random sample of N size (using N from sample_wrownum) of zips (with replacement) using all possible zips the st_cnty*/
		proc surveyselect data=zip_sub_&i.
			  outhits noprint
			  method=urs 
		      n=&num.
		      seed=31
		      out=random_samp_&i. (keep=&match_variable. &invalid_variable.);
		   strata &match_variable.;
		run;

		proc sql noprint;
			/*replace the missing zips with the random zips*/
			select &match_variable., &invalid_variable. into :var, :replacement
			from random_samp_&i.;
			
			update data_to_fix_&valid_variable.
			set &invalid_variable. = &replacement.
			where &valid_variable. = "&var." and missing(&match_variable.);
		quit;
	%end;
	
	/*delete subsetted zip data*/
	proc datasets library=work nolist;
		delete random_samp_:;
	quit;
%mend;

%macro num_random_assignment(valid_variable,invalid_variable, match_variable,indata,condition); /*valid_variable is char*/
	proc odstext;
	  p " Number of Obs with Valid &valid_variable.";
	run;

	proc sql;
		/*identify invalid zip codes from MAX data with valid state/county data*/
		create table data_to_fix_&valid_variable. as
		select bene_id,state_county, &valid_variable., &match_variable., &invalid_variable.
		from &indata.
		where &condition.;

		title Number of Obs with Valid &valid_variable.;
		select count(*) as num_valid&valid_variable.
		from data_to_fix_&valid_variable.;
		title;

		/*figure out how many of which st_cnty combos need valid zip codes*/
		create table sample as
		select distinct(&valid_variable.) as var_lookup, count(&valid_variable.) as num_assignments_needed
		from data_to_fix_&valid_variable.
		group by &valid_variable.;

		/*add row num and tot_rows for looping*/
		create table sample_wrownum as
		select *, monotonic() as row_num
		from sample;
		
		reset noprint;
		select count(*) into :tot_rows
		from sample_wrownum;
	quit;

	%do i=1 %to &tot_rows.;
		proc sql noprint;
			/*create one subset per distinct st_cnty with invalid zip data using info in sample_wrownum table*/
			select var_lookup, num_assignments_needed into :var, :num /*var = valid zip code, num = the number of random st_cnty needed*/
			from sample_wrownum
			where row_num = &i.;

			create table zip_sub_&i. as /*this table has all the st_cnty associated with that &var zip code*/
			select *
			from zips
			where &match_variable. = &var.;
		quit;

		/*create random sample of N size (using N from sample_wrownum) of zips (with replacement) using all possible zips the st_cnty*/
		proc surveyselect data=zip_sub_&i.
			  outhits noprint
			  method=urs 
		      n=&num.
		      seed=31
		      out=random_samp_&i. (keep=&match_variable. &invalid_variable.);
		   strata &match_variable.;
		run;

		proc sql noprint;
			/*replace the missing zips with the random zips*/
			select &match_variable., &invalid_variable. into :var, :replacement
			from random_samp_&i.;
			
			update data_to_fix_&valid_variable.
			set &invalid_variable. = "&replacement."
			where &valid_variable. = &var. and missing(&match_variable.);
		quit;
	%end;
	
	/*delete subsetted zip data*/
	proc datasets library=work nolist;
		delete random_samp_:;
	quit;
%mend;
%num_random_assignment(valid_variable=zip_miss,invalid_variable = st_cnty,match_variable = zip_code,indata=unmatchgeos_vzip_vcnty,condition=(valid_zip = 1 and valid_county = 0) or (valid_zip = 1 and valid_county = 1))
%char_random_assignment(valid_variable=state_county,invalid_variable = zip_code,match_variable = st_cnty,indata=unmatchgeos_vzip_vcnty,condition=valid_zip = 0 and valid_county = 1)

proc sql;
	create table replace_invalidzips as
	select a.*, b.zip_code as b_zip_code, case 
		when a.valid_zip = 0 and a.valid_county = 1 then b.zip_code
		when a.zip_miss ne . then a.zip_miss
		else .
		end as zip_fx
	from unmatchgeos_vzip_vcnty a full join data_to_fix_state_county  b
	on a.bene_id = b.bene_id;

	create table replace_invalidzips_invalidcnty as
	select a.*, b.st_cnty as b_st_cnty, put(zip_fx, 5.) as char_zip, case 
		when (a.valid_zip = 1 and a.valid_county = 0) or (a.valid_zip=1 and a.valid_county=1) then b.st_cnty
		when a.state_county ne ' ' then a.state_county
		else ' ' 
		end as cnty_fx
	from replace_invalidzips a full join data_to_fix_zip_miss b
	on a.bene_id = b.bene_id;
quit;

proc odstext;
  h "After Invalid Zip Fixes";
run;
proc freq data=replace_invalidzips_invalidcnty;
	title "After Invalid Zip Fixes";
	tables cnty_fx*b_st_cnty*zip_fx*b_zip_code/list missing nopercent;
	format cnty_fx b_st_cnty $missing_char. zip_fx b_zip_code missing_zip.;
run;

proc sql;
	create table check_zipfixes as
	select *,case
			when not missing(cnty_fx) and not missing(char_zip) then catx('-',cnty_fx,char_zip) 
			else ' '
			end as fixed_geo
	from replace_invalidzips_invalidcnty;

	create table match_fixed_geos as
	select a.*, b.zips_geo as b_zips_geo, b.zip_code as new_zip_code, b.st_cnty as new_st_cnty,
		(a.fixed_geo = b.zips_geo) as both_indic,
		(not missing(a.fixed_geo)) as in_max_only
	from check_zipfixes (drop=both_indic in_max_only) a inner join zips b
	on a.fixed_geo = b.zips_geo;
quit;
proc odstext;
  p "Verify zip and county fixes worked";
  p "Both_indic = 1 means that the county/zip combination was matched to the zips data";
run;
proc freq data=match_fixed_geos;
	title "Verify zip and county fixes worked";
	title2 "Both_indic = 1 means that the county/zip combination was matched to the zips data";
	tables both_indic*in_max_only*b_st_cnty*b_zip_code*fixed_geo*b_zips_geo/list missing nopercent;
	format b_st_cnty fixed_geo b_zips_geo $missing_char. b_zip_code missing_zip.;
run;

proc sql;
	create table all_data as
	select *
	from matched_geos
	union corr
	select *
	from match_fixed_geos;
quit;

proc contents data=all_data;run;
ods excel close;

proc sql;
	create table person_level_child as
	select *
	from all_data
	where age_cat=1 and not missing(cell);

	create table person_level_child_msg as
	select *
	from all_data
	where age_cat=1 and missing(cell);

	create table person_level_adult as
	select *
	from all_data
	where age_cat=2 and not missing(cell);

	create table person_level_adult_msg as
	select *
	from all_data
	where age_cat=2 and missing(cell);

	create table person_level_senior as
	select *
	from all_data
	where age_cat=3 and not missing(cell);

	create table person_level_senior_msg as
	select *
	from all_data
	where age_cat=3 and missing(cell);
quit;

%macro get_sum_tables(indata,outdata);
	proc sql;
		create table &outdata. as
		select cell_type1, cell_type2, cell_type3, cell_type4, cell, age_cat, 
			sum(cell_n) as cell_n, 
	        sum(d_cell_n) as d_cell_n, 
			sum(pm_n) as pm_n,
	        sum(male) as male, 
	        sum(died_n) as died_n,
			sum(age_0) as _0, 
	        sum(age_1_5) as _1_5, 
	        sum(age_6_18) as _6_18, 
	        sum(age_19_44) as _19_44, 
	        sum(age_45_64) as _45_64, 
	        sum(age_65_84) as _65_84, 
	        sum(age_85p) as _85p,

			sum(mas_cash) as mas_cash,
	        sum(mas_mn) as mas_mn, 
	        sum(mas_pov) as mas_pov, 
	        sum(mas_oth) as mas_oth, 
	        sum(mas_1115) as mas_1115,
			sum(boe_aged) as boe_aged, 
	        sum(boe_disabled) as boe_disabled, 
	        sum(boe_child) as boe_child, 
	        sum(boe_adult) as boe_adult, 
			sum(boe_uchild) as boe_uchild, 
	        sum(boe_uadult) as boe_uadult, 
	        sum(boe_fchild) as boe_fchild, 

	        sum(TOT_MDCD_PYMT_AMT) as spending, 
			sum(EL_ELGBLTY_MO_CNT) as elg_months, 
	        sum(mo_dual) as mo_dual, 
	        sum(mo_mc) as mo_mc, 
	        sum(mo_dsbl) as mo_dsbl, 
	        sum(mo_ltss) as mo_ltss
			from &indata.
	      	group by cell_type1, cell_type2, cell_type3, cell_type4, cell, age_cat;
	quit;

	data &outdata.;
		set &outdata;
		label
			cell_type1="Medicaid Only, Dual, or Foster Care"
			cell_type2="MC, FFS, or Foster Care"
			cell_type3="Disability, No Disability, or Foster Care"
			cell_type4="LTSS, No LTSS, or Foster Care"
			cell="MAS/BOE/Foster Care Category"
			age_cat="Age Category"
			cell_n="Number of Beneficiaries"
			d_cell_n="Number of Unique Statuses"
			pm_n="Number of enrollment months during year"
			male="Number of Male Beneficiaries"
			died_n="Number Dying in Year"
			mas_cash="MAS Cash Beneficiaries"
			mas_mn="MAS Medically Needy Beneficiaries"
			mas_pov="MAS Poverty-Related Beneficiaries"
			mas_oth="MAS Other Beneficiaries"
			mas_1115="MAS 1115 Exspansion Beneficiaries"
			boe_aged="BOE Aged Beneficiaries"
			boe_disabled="BOE Disabled Beneficiaries"
			boe_child="BOE Child Beneficiaries"
			boe_adult="BOE Adult Beneficiaries"
			boe_uchild="BOE Child (Unemployed Adult) Beneficiaries"
			boe_uadult="BOE Unemployed Adult Beneficiaries"
			boe_fchild="BOE Foster Child Beneficiaries"
			spending="Total Annual Spending across Beneficiaries"

			elg_months="Number of Person Months of Eligibility"
			_0="Number of Beneficiaries Age less than 1 year"
			_1_5="Number of Beneficiaries Age 1 to 5"
			_6_18="Number of Beneficiaries Age 6 to 18"
			_19_44="Number of Beneficiaries Age 19 to 44"
			_45_64="Number of Beneficiaries Age 45 to 64"
			_65_84="Number of Beneficiaries Age 65 to 84"
			_85p="Number of Beneficiaries Age 85 and above"
			mo_dual="Number of Person Months of Dual Eligibility"
			mo_mc="Number of Person Months of Managed Care Enrollment"
			mo_dsbl="Number of Person Months of Disability"
			mo_ltss="Number of Person Months of LTSS Use"
			mc_cat="Managed Care Category"
			dis_cat="Disability Category"
			dual_cat="Dual-Eligibility Category"
			ltss_cat="LTSS Use Category"
			foster_cat="Foster Care Category"
			;
		run;
	options nolabel;
	proc sql;
		select *
		from &outdata.
		order by cell;
	quit;
	options label;
%mend;

%macro get_rate_tables(indata,outdata);
	proc sql;
		create table &outdata. as
		 select cell_type1, cell_type2, cell_type3, cell_type4, cell, age_cat, 
			sum(cell_n) as cell_n, 
	        sum(d_cell_n)/sum(cell_n) as d_cell_rt format=8.3,
			sum(pm_n)/sum(cell_n) as pm_rt format=8.3,
	        sum(male)/sum(cell_n) as male_rt format=8.3, 
	        sum(died_n)/sum(cell_n) as died_rt format=8.3, 
			sum(age_0)/sum(cell_n) as _0_rt format=8.3, 
	        sum(age_1_5)/sum(cell_n) as _1_5_rt format=8.3, 
	        sum(age_6_18)/sum(cell_n) as _6_18_rt format=8.3, 
	        sum(age_19_44)/sum(cell_n) as _19_44_rt format=8.3, 
	        sum(age_45_64)/sum(cell_n) as _45_64_rt format=8.3,  
	        sum(age_65_84)/sum(cell_n) as _65_84_rt format=8.3,  
	        sum(age_85p)/sum(cell_n) as _85p_rt format=8.3, 

			sum(mas_cash)/sum(cell_n) as mas_cash_rt format=8.3, 
	        sum(mas_mn)/sum(cell_n) as mas_mn_rt format=8.3, 
	        sum(mas_pov)/sum(cell_n) as mas_pov_rt format=8.3, 
	        sum(mas_oth)/sum(cell_n) as mas_oth_rt format=8.3,  
	        sum(mas_1115)/sum(cell_n) as mas_1115_rt format=8.3, 
			sum(boe_aged)/sum(cell_n) as boe_aged_rt format=8.3,  
	        sum(boe_disabled)/sum(cell_n) as boe_disabled_rt format=8.3, 
	        sum(boe_child)/sum(cell_n) as boe_child_rt format=8.3, 
	        sum(boe_adult)/sum(cell_n) as boe_adult_rt format=8.3,  
			sum(boe_uchild)/sum(cell_n) as boe_uchild_rt format=8.3, 
	        sum(boe_uadult)/sum(cell_n) as boe_uadult_rt format=8.3,  
	        sum(boe_fchild)/sum(cell_n) as boe_fchild_rt format=8.3,  

	        sum(TOT_MDCD_PYMT_AMT)/sum(cell_n) as spending_rt format=8.3, 
			sum(EL_ELGBLTY_MO_CNT)/sum(cell_n) as elg_months_rt format=8.3, 
	        sum(mo_dual)/sum(cell_n) as mo_dual_rt format=8.3, 
	        sum(mo_mc)/sum(cell_n) as mo_mc_rt format=8.3, 
	        sum(mo_dsbl)/sum(cell_n) as mo_dsbl_rt format=8.3, 
	        sum(mo_ltss)/sum(cell_n) as mo_ltss_rt format=8.3
			from &indata.
	      	group by cell_type1, cell_type2, cell_type3, cell_type4, cell, age_cat;
	data &outdata.;
		set &outdata.;
		label
			cell_type1="Medicaid Only, Dual, or Foster Care"
			cell_type2="MC, FFS, or Foster Care"
			cell_type3="Disability, No Disability, or Foster Care"
			cell_type4="LTSS, No LTSS, or Foster Care"
			cell="MAS/BOE/Foster Care Category"
			age_cat="Age Category"
			cell_n="Number of Beneficiaries"
			d_cell_rt="Rate of Unique Statuses"
			pm_rt="Rate of enrollment months during year"
			male_rt="Rate of Male Beneficiaries"
			died_rt="Rate Dying in Year"
			mas_cash_rt="Rate of MAS Cash Beneficiaries"
			mas_mn_rt="Rate of MAS Medically Needy Beneficiaries"
			mas_pov_rt="Rate of MAS Poverty-Related Beneficiaries"
			mas_oth_rt="Rate of MAS Other Beneficiaries"
			mas_1115_rt="Rate of MAS 1115 Exspansion Beneficiaries"
			boe_aged_rt="Rate of BOE Aged Beneficiaries"
			boe_disabled_rt="Rate of BOE Disabled Beneficiaries"
			boe_child_rt="Rate of BOE Child Beneficiaries"
			boe_adult_rt="Rate of BOE Adult Beneficiaries"
			boe_uchild_rt="Rate of BOE Child (Unemployed Adult) Beneficiaries"
			boe_uadult_rt="Rate of BOE Unemployed Adult Beneficiaries"
			boe_fchild_rt="Rate of BOE Foster Child Beneficiaries"
			spending_rt="Rate of Total Annual Spending across Beneficiaries"

			elg_months_rt="Rate of Person Months of Eligibility"
			_0_rt="Rate of Beneficiaries Age less than 1 year"
			_1_5_rt="Rate of Beneficiaries Age 1 to 5"
			_6_18_rt="Rate of Beneficiaries Age 6 to 18"
			_19_44_rt="Rate of Beneficiaries Age 19 to 44"
			_45_64_rt="Rate of Beneficiaries Age 45 to 64"
			_65_84_rt="Rate of Beneficiaries Age 65 to 84"
			_85p_rt="Rate of Beneficiaries Age 85 and above"
			mo_dual_rt="Rate of Person Months of Dual Eligibility"
			mo_mc_rt="Rate of Person Months of Managed Care Enrollment"
			mo_dsbl_rt="Rate of Person Months of Disability"
			mo_ltss_rt="Rate of Person Months of LTSS Use"
			mc_cat_rt="Rate of Managed Care Category"
			dis_cat_rt="Rate of Disability Category"
			dual_cat_rt="Rate of Dual-Eligibility Category"
			ltss_cat_rt="Rate of LTSS Use Category"
			foster_cat_rt="Rate of Foster Care Category"

			;
		run;
	options nolabel;
		proc sql;
			select *
			from &outdata.
			order by cell;
		quit;
		options label;
	quit;
%mend;


ods excel file="C:\Users\LDURBAK\Documents\max stats_national_&fname..xlsx";
ods excel options(sheet_name="child_raw" sheet_interval="none");
%get_sum_tables(indata=person_level_child,outdata=child_sum);

/*Add dummy table*/
ods excel options(sheet_interval="table");
ods exclude all;
data _null_;
file print;
put _all_;
run;
ods select all;
ods excel options(sheet_name="adult_raw" sheet_interval="none");
%get_sum_tables(indata=person_level_adult,outdata=adult_sum);

/*Add dummy table*/
ods excel options(sheet_interval="table");
ods exclude all;
data _null_;
file print;
put _all_;
run;
ods select all;
ods excel options(sheet_name="senior_raw" sheet_interval="none");
%get_sum_tables(indata=person_level_senior,outdata=senior_sum);

/*Add dummy table*/
ods excel options(sheet_interval="table");
ods exclude all;
data _null_;
file print;
put _all_;
run;
ods select all;
ods excel options(sheet_name="child_rates" sheet_interval="none");
%get_rate_tables(indata=person_level_child,outdata=child_rate);

/*Add dummy table*/
ods excel options(sheet_interval="table");
ods exclude all;
data _null_;
file print;
put _all_;
run;
ods select all;
ods excel options(sheet_name="adult_rates" sheet_interval="none");
%get_rate_tables(indata=person_level_adult,outdata=adult_rate);

/*Add dummy table*/
ods excel options(sheet_interval="table");
ods exclude all;
data _null_;
file print;
put _all_;
run;
ods select all;
ods excel options(sheet_name="senior_rates" sheet_interval="none");
%get_rate_tables(indata=person_level_senior,outdata=senior_rate);

/*Add dummy table*/
ods excel options(sheet_interval="table");
ods exclude all;
data _null_;
file print;
put _all_;
run;
ods select all;

ods excel options(sheet_name="dictionary" sheet_interval="none");
proc sql;
	select name label="Variable", "" label="Related Variables", label label="Label", "" as denom label="Denominator", "" label="Value Label", "" label="Notes"
	from dictionary.columns
	where libname = "WORK" and memname = "CHILD_SUM"
	union
	select name label="Variable", "" label="Related Variables", label label="Label", case
				when name in ("d_cell_rt", "pm_rt","male_rt","died_rt","mcd_full_rt","mas_cash_rt","mas_mn_rt","mas_pov_rt",
								"mas_oth_rt","mas_1115_rt","boe_aged_rt","boe_disabled_rt","boe_child_rt","boe_adult_rt",
								"boe_uchild_rt","boe_uadult_rt","boe_fchild_rt","spending_rt","age_rt","_0_rt","_1_5_rt","_6_18_rt","_19_44_rt",
								"_45_64_rt","_65_84_rt","_85p_rt",) then "cell_n"
				when name in ("disabled_rt","cash_rt","chip_rt","ltss_rt") then "pm_n"
				else ""
			end as denom label = "Demoninator",
			"" label="Value Label", "" label="Notes"
	from dictionary.columns
	where libname = "WORK" and memname = "CHILD_RATE"
	order by denom, name;
quit;

ods excel close;


ods excel file="C:\Users\LDURBAK\Documents\max stats_national_msg_&fname..xlsx";
ods excel options(sheet_name="child_raw" sheet_interval="none");
%get_sum_tables(indata=person_level_child_msg,outdata=child_sum_msg);

/*Add dummy table*/
ods excel options(sheet_interval="table");
ods exclude all;
data _null_;
file print;
put _all_;
run;
ods select all;
ods excel options(sheet_name="adult_raw" sheet_interval="none");
%get_sum_tables(indata=person_level_adult_msg,outdata=adult_sum_msg);

/*Add dummy table*/
ods excel options(sheet_interval="table");
ods exclude all;
data _null_;
file print;
put _all_;
run;
ods select all;
ods excel options(sheet_name="senior_raw" sheet_interval="none");
%get_sum_tables(indata=person_level_senior_msg,outdata=senior_sum_msg);

/*Add dummy table*/
ods excel options(sheet_interval="table");
ods exclude all;
data _null_;
file print;
put _all_;
run;
ods select all;
ods excel options(sheet_name="child_rates" sheet_interval="none");
%get_rate_tables(indata=person_level_child_msg,outdata=child_rate_msg);

/*Add dummy table*/
ods excel options(sheet_interval="table");
ods exclude all;
data _null_;
file print;
put _all_;
run;
ods select all;
ods excel options(sheet_name="adult_rates" sheet_interval="none");
%get_rate_tables(indata=person_level_adult_msg,outdata=adult_rate_msg);

/*Add dummy table*/
ods excel options(sheet_interval="table");
ods exclude all;
data _null_;
file print;
put _all_;
run;
ods select all;
ods excel options(sheet_name="senior_rates" sheet_interval="none");
%get_rate_tables(indata=person_level_senior_msg,outdata=senior_rate_msg);

/*Add dummy table*/
ods excel options(sheet_interval="table");
ods exclude all;
data _null_;
file print;
put _all_;
run;
ods select all;

ods excel options(sheet_name="dictionary" sheet_interval="none");
proc sql;
	select name label="Variable", "" label="Related Variables", label label="Label", "" as denom label="Denominator", "" label="Value Label", "" label="Notes"
	from dictionary.columns
	where libname = "WORK" and memname = "CHILD_SUM_MSG"
	union
	select name label="Variable", "" label="Related Variables", label label="Label", case
				when name in ("d_cell_rt", "pm_rt","male_rt","died_rt","mcd_full_rt","mas_cash_rt","mas_mn_rt","mas_pov_rt",
								"mas_oth_rt","mas_1115_rt","boe_aged_rt","boe_disabled_rt","boe_child_rt","boe_adult_rt",
								"boe_uchild_rt","boe_uadult_rt","boe_fchild_rt","spending_rt","age_rt","_0_rt","_1_5_rt","_6_18_rt","_19_44_rt",
								"_45_64_rt","_65_84_rt","_85p_rt",) then "cell_n"
				when name in ("disabled_rt","cash_rt","chip_rt","ltss_rt") then "pm_n"
				else ""
			end as denom label = "Demoninator",
			"" label="Value Label", "" label="Notes"
	from dictionary.columns
	where libname = "WORK" and memname = "CHILD_RATE_MSG"
	order by denom, name;
quit;

ods excel close;

proc sql;
	create table space.id_pop_&fname. as
	select *
	from temp;
quit;
/*
*Print summary stats out to excel
data person_level_num (keep=_NUMERIC_) person_level_char (keep=_CHARACTER_);
	set person_level;
run;

ods excel file="C:\Users\LDURBAK\Documents\personlevel_initial_summary.xlsx";
ods excel options(sheet_name="Numeric Summary Stats" sheet_interval="none");
proc means data=person_level_num stackodsoutput  maxdec=2 n nmiss mean std min max;
	title "person_level numeric summary stats";
run;
* Add dummy table
ods excel options(sheet_interval="table");
ods exclude all;
data _null_;
file print;
put _all_;
run;
ods select all;

ods excel options(sheet_name="Categorical N Levels & Freq Tables" sheet_interval="none");
ods output nlevels=want_dataset;
proc freq data=person_level_char nlevels;
	title "person-level nlevels";
	tables _all_;
	format bene_id $missfmt.;
run;

* Add dummy table
ods excel options(sheet_interval="table");
ods exclude all;
data _null_;
file print;
put _all_;
run;
ods select all;

ods excel options(sheet_name="Cell Assignment" sheet_interval="none");
proc freq data=person_level;
	title "Cell vs. Month Comparison";
	tables month_1*month_2*month_3* month_4*month_5*month_6* month_7*month_8*month_9* month_10*month_11*month_12*cell_1*cell_2*cell_3*cell_4*cell_5*cell_6*cell_7*cell_8*cell_9*cell_10*cell_11*cell_12*cell_13*cell_14*cell_15*cell_16*cell_17/list;
run;

* Add dummy table
ods excel options(sheet_interval="table");
ods exclude all;
data _null_;
file print;
put _all_;
run;
ods select all;

ods excel options(sheet_name="Record and Distinct Bene_ID Counts" sheet_interval="none");

proc sql noprint;
	select name || 'in ("nmcd","msg","chip")' into :in_condition separated by " or "
	from dictionary.columns
	where libname="WORK" and memname = "PERSON_LEVEL" and name like "month_%";

	select name || 'not in ("nmcd","msg","chip")' into :notin_condition separated by " and "
	from dictionary.columns
	where libname="WORK" and memname = "PERSON_LEVEL" and name like "month_%";
quit;

proc sql;
	select count(*) as max_ps_2012_rec_count, count(distinct bene_id) as max_ps_2012_dist_bene_ids
	from max_data_in;

	select count(*) as person_level_rec_count, count(distinct bene_id) as person_level_distinct_bene_ids
	from person_level;

	select count(*) as num_missing_bene_id_to_drop
	from person_level
	where bene_id = ' ';

	select count(*) as num_ncmd_msg_chip_to_drop
	from person_level
	where &in_condition;

	select count(*) as num_missing_zip_to_drop
	from person_level
	where EL_RSDNC_CNTY_CD_LTST in('000','999','');

	select count(*) as num_to_drop_total
	from person_level
	where bene_id = ' ' or &in_condition or EL_RSDNC_CNTY_CD_LTST in('000','999','');

	create table elig_person_level as
	select *
	from person_level
	where bene_id ne ' ' and &notin_condition and EL_RSDNC_CNTY_CD_LTST not in('000','999','');

	select count(*) as elig_perslvl_rec_count, count(distinct bene_id) as elig_perslvl_distinct_bene_ids
	from elig_person_level; 
quit;
ods excel close; 

*/
