/* 
   Create a dataset with desired MAX variables for given cells at the  county and national level,
    based on latest eligible month, and merge with AHRF county-level data

   Author: Leah Durbak (based on code by Abby Norling-Ruggles, modified by Kyle Caswell, modified by Tim Waidmann)

*/

* Date for version control;
*%let date=10_30_2018;

* log;
*PROC PRINTTO PRINT="P:\MCD-SPVR\log\02_max_ahrf_&sysdate..lst"
               LOG="P:\MCD-SPVR\log\02_max_ahrf_&sysdate..log" NEW;
*RUN;

libname data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname space   "P:\MCD-SPVR\data\workspace";
libname area    "P:\MCD-SPVR\data\NO_PII";
libname out     "P:\MCD-SPVR\data\workspace\output";
libname library "P:\MCD-SPVR\data\workspace"; * includes format file;

options obs=10000;

proc format library=library;
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
run;

/* macro to calculate month-specific variables */ 
%macro cells();

/* create person-level dataset with appropriate variables and cell types */
data person_level;
  set data.maxdata_ps_2012 (keep = bene_id state_cd
		EL_RSDNC_ZIP_CD_LTST EL_RSDNC_CNTY_CD_LTST EL_DOB MC_COMBO_MO:
		EL_MDCR_DUAL_MO_: EL_RSTRCT_BNFT_FLG: MAX_ELG_CD_MO: EL_CHIP_FLAG: 
		EL_SEX_CD TOT_MDCD_PYMT_AMT EL_ELGBLTY_MO_CNT EL_MAX_ELGBLTY_CD_LTST
		EL_DOD MDCR_DOD CLTC: FEE_FOR_SRVC_IND_07);

  	/****************************/
	/*Variable creation/recoding*/
	/****************************/
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
		else age_0=0;
	if age>=1 and age<=5 then age_1_5=1;
		else age_1_5=0;
	if age>=6 and age<=18 then age_6_18=1;
		else age_6_18=0;
	if age>=19 and age<=44 then age_19_44=1;
		else age_19_44=0;
	if age>=45 and age<=64 then age_45_64=1;
		else age_45_64=0;
	if age>=65 and age <=84 then age_65_84=1;
		else age_65_84=0;
	if age>=85 then age_85p=1;
		else age_85p=0;

	if cell='1' then do; 		cell='01'; dual_cat=0; mc_cat=1; dis_cat=1; ltss_cat=1; foster_cat=0; end;
	else if cell='2' then do; 	cell='02'; dual_cat=0; mc_cat=1; dis_cat=1; ltss_cat=0; foster_cat=0; end;
	else if cell='3' then do; 	cell='03'; dual_cat=0; mc_cat=1; dis_cat=0; ltss_cat=1; foster_cat=0; end;
	else if cell='4' then do; 	cell='04'; dual_cat=0; mc_cat=1; dis_cat=0; ltss_cat=0; foster_cat=0; end;
	else if cell='5' then do; 	cell='05'; dual_cat=0; mc_cat=0; dis_cat=1; ltss_cat=1; foster_cat=0; end;
	else if cell='6' then do; 	cell='06'; dual_cat=0; mc_cat=0; dis_cat=1; ltss_cat=0; foster_cat=0; end;
	else if cell='7' then do; 	cell='07'; dual_cat=0; mc_cat=0; dis_cat=0; ltss_cat=1; foster_cat=0; end;
	else if cell='8' then do; 	cell='08'; dual_cat=0; mc_cat=0; dis_cat=0; ltss_cat=0; foster_cat=0; end;
	else if cell='9' then do; 	cell='09'; dual_cat=1; mc_cat=1; dis_cat=1; ltss_cat=1; foster_cat=0; end;
	else if cell='10' then do; 		   dual_cat=1; mc_cat=1; dis_cat=1; ltss_cat=0; foster_cat=0; end;
	else if cell='11' then do;		   dual_cat=1; mc_cat=1; dis_cat=0; ltss_cat=1; foster_cat=0; end;
	else if cell='12' then do;		   dual_cat=1; mc_cat=1; dis_cat=0; ltss_cat=0; foster_cat=0; end;
	else if cell='13' then do;		   dual_cat=1; mc_cat=0; dis_cat=1; ltss_cat=1; foster_cat=0; end;
	else if cell='14' then do;		   dual_cat=1; mc_cat=0; dis_cat=1; ltss_cat=0; foster_cat=0; end;
	else if cell='15' then do;		   dual_cat=1; mc_cat=0; dis_cat=0; ltss_cat=1; foster_cat=0; end;
	else if cell='16' then do;		   dual_cat=1; mc_cat=0; dis_cat=0; ltss_cat=0; foster_cat=0; end;
	else if cell='17' then do; 	age_cat=1; dual_cat=9; mc_cat=9; dis_cat=9; ltss_cat=9; foster_cat=1; end;
	if not missing(cell) then do;
	if age_cat=1 then cell_age=catx('_','child',cell);
		else if age_cat=2 then cell_age=catx('_','adult',cell);
	   	else if age_cat=3 then cell_age=catx('_','senior',cell);
	end;

	/*****************/
  	/*geo corrections*/
	/*****************/
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

    /*create missing flags */
	if missing(age) then _age=1;
	else _age=0;

	if missing(male) then _male=1;
	else _male=0;

	if missing(died_n) then _died=1;
	else _died=0;

	/* create death flag */
	if missing(EL_DOD) and missing(MDCR_DOD) then died_n=0;
	else died_n=1;

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

		format cell $15.;
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
run;

%mend cells;

%cells;

/*additional geo fixes*/
proc sort data=area.all_zips;
	by zip_code;
run;

data max_zip_merge; *.........................................................................;
*data space.max_zip_merge;
   merge area.all_zips(in=a rename=(state_cd=state)) zip_categories(in=b);
   by zip_code;
   if a=1 and not(b=1) then _merge=1;
   if b=1 and not(a=1) then _merge=2;
   if a=1 and b=1 then _merge=3;
   if state_cd ~= state then wrong_zip=1;
   	else wrong_zip=0;
run;

data invalid_zip;
	set max_zip_merge; *.........................................................................;
	*set space.max_zip_merge;
	where _merge=2 and not missing(EL_RSDNC_CNTY_CD_LTST) and EL_RSDNC_CNTY_CD_LTST ~in('000','999');
	st_cnty = catx('-',state_cd,EL_RSDNC_CNTY_CD_LTST);
run;

data valid_zip;
    set max_zip_merge; *.........................................................................;
	*set space.max_zip_merge;
	where _merge=3 and wrong_zip=0;
run;

proc sql;
  *make list of counties;
  create table cty_list as
  select state_cd, county_fips, count(county_fips) as count, catx('-',state_cd,county_fips) as st_cnty
  from area.all_zips
  group by state_cd, county_fips;

  *select observations with invalid zips but valid counties;
  create table invalid_zip_ctygood as 
  select invalid_zip.*, cty_list.*
  from invalid_zip inner join cty_list
  on invalid_zip.st_cnty = cty_list.st_cnty;
quit;

proc sql;
  create table zip_strata as
  select state_cd, 
  EL_RSDNC_CNTY_CD_LTST as county_fips, 
  catx('-',state_cd,EL_RSDNC_CNTY_CD_LTST) as st_cnty, 
  count(EL_RSDNC_CNTY_CD_LTST) as _nsize_
  from invalid_zip_ctygood
  group by state_cd, EL_RSDNC_CNTY_CD_LTST;
quit;

proc sort data=area.all_zips;
  by st_cnty;
run;

*select random zip codes based on county;
proc surveyselect data=area.all_zips
      method=urs 
      n=zip_strata
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

data invalid_combine;
	set invalid_zip_ctygood;
	drop zip_code;
run;

proc sort data=invalid_combine;
	by st_cnty;
run;

* merge new zip codes onto invalid zips dataset;
data imputed_zips;
	set zip_long;
	set invalid_combine;
	zip_code=input(zip,best12.);
run;

*merge new zip file with valid zips;
data max_zip_complete;
	set imputed_zips valid_zip;
run;
* kjc: PATCH HERE TO THROW OUT THOSE WITH MISSING INFO ON KEY VARS ***********************************************;
data max_zip_complete;
  set max_zip_complete;
  where not missing(cell) & not missing(age_cat) & not missing(state_cd) & not missing(zip_code);
  test=sum(age_0,age_1_5,age_6_18,age_19_44,age_45_64,age_65_84,age_85p);
  if test=1;
run;


/**********************************
/*Print summary stats out to excel*
/**********************************
data person_level_num (keep=_NUMERIC_) person_level_char (keep=_CHARACTER_);
	set person_level;
run;

ods excel file="C:\Users\LDURBAK\Documents\personlevel_initial_summary.xlsx";
ods excel options(sheet_name="Numeric Summary Stats" sheet_interval="none");
proc means data=person_level_num stackodsoutput  maxdec=2 n nmiss mean std min max;
	title "person_level numeric summary stats";
run;

* Add dummy table *
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
