
%let indata=data.maxdata_ps_2012; %let outdata=personlevel;
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
	data temp1 ;
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
		if age>=1 and age<=5 then age_1_5=1;
		if age>=6 and age<=18 then age_6_18=1;
		if age>=19 and age<=44 then age_19_44=1;
		if age>=45 and age<=64 then age_45_64=1;
		if age>=65 and age <=84 then age_65_84=1;
		if age>=85 then age_85p=1;

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
	%let dual_full_dum_missing=;
	%dum_var(indata=temp1,
		month_var=EL_MDCR_DUAL_MO_,
		condition_one="in ('09','59','00','50')",
		condition_zero="not in ('09','59','00','50')",
		dum_var_pref=_dual_full_,
		macro_var_name=dual_full_dum_missing);

	%let mcd_full_dum_missing=;
	%dum_var(indata=temp1,
		month_var=EL_RSTRCT_BNFT_FLG_,
		condition_one="in ('9','')",
		condition_zero="not in ('9','')",
		dum_var_pref=_mcd_full_,
		macro_var_name=mcd_full_dum_missing);

	%let elig_dum_missing=;
	%dum_var(indata=temp1,
		month_var=MAX_ELG_CD_MO_,
		condition_one="in ('99','')",
		condition_zero="not in ('99','')",
		dum_var_pref=_elig_,
		macro_var_name=elig_dum_missing);

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
			 &dual_full_dum_missing.,
			 &mcd_full_dum_missing.,
			 &elig_dum_missing.,
			 &disabled_dum.,
			 &chip_dum.,
			 &chip_missing_dum.,
			 &nmcd_dum.,
			 &foster_dum.
		from temp1;
	quit;

	%macro create_servicetype_cat(month_suff=);
		case 
			when chip_&month_suff. = 1 then "chip"
			when nmcd_&month_suff. = 1 then "nmcd"
			when foster_&month_suff. = 1 then "17"
			/*when mc_&month_suff.=1 or _dual_&month_suff.=1 or (_dual_full_&month_suff.=1 and _mcd_full_&month_suff.=1) or _elig_&month_suff.=1 or _chip_&month_suff.=1 or _ltss=1 then "msg"*/
			when (dual_&month_suff. = 0 and mc_&month_suff.=1 and disabled_&month_suff.=1 and ltss=1) then "01" 
			when (dual_&month_suff. = 0 and mc_&month_suff.=1 and disabled_&month_suff.=1 and ltss=0) then "02" 
			when (dual_&month_suff. = 0 and mc_&month_suff.=1 and disabled_&month_suff.=0 and ltss=1) then "03" 
			when (dual_&month_suff. = 0 and mc_&month_suff.=1 and disabled_&month_suff.=0 and ltss=0) then "04" 
			when (dual_&month_suff. = 0 and mc_&month_suff.=0 and disabled_&month_suff.=1 and ltss=1) then "05" 
			when (dual_&month_suff. = 0 and mc_&month_suff.=0 and disabled_&month_suff.=1 and ltss=0) then "06" 
			when (dual_&month_suff. = 0 and mc_&month_suff.=0 and disabled_&month_suff.=0 and ltss=1) then "07" 
			when (dual_&month_suff. = 0 and mc_&month_suff.=0 and disabled_&month_suff.=0 and ltss=0) then "08" 
			when (dual_&month_suff. = 1 and mc_&month_suff.=1 and disabled_&month_suff.=1 and ltss=1) then "09" 
			when (dual_&month_suff. = 1 and mc_&month_suff.=1 and disabled_&month_suff.=1 and ltss=0) then "10" 
			when (dual_&month_suff. = 1 and mc_&month_suff.=1 and disabled_&month_suff.=0 and ltss=1) then "11" 
			when (dual_&month_suff. = 1 and mc_&month_suff.=1 and disabled_&month_suff.=0 and ltss=0) then "12" 
			when (dual_&month_suff. = 1 and mc_&month_suff.=0 and disabled_&month_suff.=1 and ltss=1) then "13" 
			when (dual_&month_suff. = 1 and mc_&month_suff.=0 and disabled_&month_suff.=1 and ltss=0) then "14" 
			when (dual_&month_suff. = 1 and mc_&month_suff.=0 and disabled_&month_suff.=0 and ltss=1) then "15" 
			when (dual_&month_suff. = 1 and mc_&month_suff.=0 and disabled_&month_suff.=0 and ltss=0) then "16" 
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
			array eligmo_servicetypes{*} 3. elig_months_1-elig_months_17;
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

			if servicetype_12 not in ('chip','nmcd','','msg') then latest_servicetype = coalescec(servicetype_12,servicetype_11,servicetype_10,servicetype__9,servicetype__8,servicetype__7,
																		servicetype__6,servicetype__5,servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype_11 not in ('chip','nmcd','','msg') then latest_servicetype = coalescec(servicetype_11,servicetype_10,servicetype__9,servicetype__8,servicetype__7,
																		servicetype__6,servicetype__5,servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype_10 not in ('chip','nmcd','','msg') then latest_servicetype = coalescec(servicetype_10,servicetype__9,servicetype__8,servicetype__7,
																		servicetype__6,servicetype__5,servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype__9 not in ('chip','nmcd','','msg') then latest_servicetype = coalescec(servicetype__9,servicetype__8,servicetype__7,
																		servicetype__6,servicetype__5,servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype__8 not in ('chip','nmcd','','msg') then latest_servicetype = coalescec(servicetype__8,servicetype__7,
																		servicetype__6,servicetype__5,servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype__7 not in ('chip','nmcd','','msg') then latest_servicetype = coalescec(servicetype__7,
																		servicetype__6,servicetype__5,servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype__6 not in ('chip','nmcd','','msg') then latest_servicetype = coalescec(servicetype__6,servicetype__5,servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype__5 not in ('chip','nmcd','','msg') then latest_servicetype = coalescec(servicetype__5,servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype__4 not in ('chip','nmcd','','msg') then latest_servicetype = coalescec(servicetype__4,servicetype__3,servicetype__2,servicetype__1);
			else if servicetype__3 not in ('chip','nmcd','','msg') then latest_servicetype = coalescec(servicetype__3,servicetype__2,servicetype__1);
			else if servicetype__2 not in ('chip','nmcd','','msg') then latest_servicetype = coalescec(servicetype__2,servicetype__1);
			else if servicetype__1 not in ('chip','nmcd','','msg') then latest_servicetype = servicetype__1;

			if age_cat=1 then age_servicetype = catx('_','child',latest_servicetype); 
			else if age_cat=2 then age_servicetype = catx('_','adult',latest_servicetype); 
			else if age_cat=3 then age_servicetype = catx('_','senior',latest_servicetype);

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
			else if latest_servicetype='17' then do;		dual_cat=9; mc_cat=9; dis_cat=9; ltss_cat=9; foster_cat=1; end;
			else if latest_servicetype='chip' then do;		dual_cat=8; mc_cat=8; dis_cat=8; ltss_cat=8; foster_cat=8; end;
			else if latest_servicetype='nmcd' then do;		dual_cat=7; mc_cat=7; dis_cat=7; ltss_cat=7; foster_cat=7; end;
			else if latest_servicetype='' then do;		dual_cat=6; mc_cat=6; dis_cat=6; ltss_cat=6; foster_cat=6; end;
	
		run;
proc freq data=temp3;
	table servicetype_:/list missing;
run;

%cells(indata=data.maxdata_ps_2012, outdata=personlevel);

