   %include 'g:\logparse\logparse.sas';
  
   libname pdata "g:\logparse";

%logparse(G:\dynasim\programs\run949\Opt0\readfeh2090v4.log, pdata.perfdata, OTH, APPEND=NO);                          	/* read feh file into sas */
%logparse(g:\dynasim\programs\run949\Opt0\MakeDependents.log, pdata.perfdata, OTH, APPEND=YES);                         /* construct longitudinal family and dependent info */
%logparse(g:\dynasim\programs\run949\Opt0\allpers.log, pdata.perfdata, OTH, APPEND=YES);                                /* create random number seeds, make random access ID file */
%logparse(g:\dynasim\programs\run949\Opt0\AnnuityFile.log, pdata.perfdata, OTH, APPEND=YES);                            /* make MINT variables and dody */
%logparse(g:\dynasim\programs\run949\Opt0\pvMint.log, pdata.perfdata, OTH, APPEND=YES);                                 /* Calculate annuity factors--rerun if marital status changes */
%logparse(G:\Dynasim\programs\run949\Opt0\PensionM7jobchangev32.log, pdata.perfdata, OTH, APPEND=YES);                  /* calculate jobchange and pensions */
%logparse(g:\dynasim\programs\run949\Opt0\ssb_program_runOpt0.log, pdata.perfdata, OTH, APPEND=YES);                    /* calculate social security benefits */
%logparse(g:\dynasim\programs\run949\Opt0\mint3residV3.log, pdata.perfdata, OTH, APPEND=YES);                           /* calculate initial predicted wealth */
%logparse(g:\dynasim\programs\run949\Opt0\errorHomeVersion2Adjusted.log, pdata.perfdata, OTH, APPEND=YES);              /* create home wealth individual-specific error terms */
%logparse(g:\dynasim\programs\run949\Opt0\errorTwealth.log, pdata.perfdata, OTH, APPEND=YES);                           /* create fin wealth individual-specific error terms */
%logparse(g:\dynasim\programs\run949\Opt0\projectallv4base.log, pdata.perfdata, OTH, APPEND=YES);                       /* project assets */
%logparse(g:\dynasim\programs\run949\Opt0\MakeSpouseOPT0.log, pdata.perfdata, OTH, APPEND=YES);                         /* create a random access spouse file to use in income program */
%logparse(G:\dynasim\programs\run949\Opt0\income2050_Opt0SaverCreditV7old_tax.log, pdata.perfdata, OTH, APPEND=YES);    /* final income program */

%logparse(G:\dynasim\programs\run949\Opt0\BPCtabsOPT0.log, pdata.perfdata, OTH, APPEND=YES);
%logparse(G:\dynasim\programs\run949\Opt0\BPCpayrollOPT0.log, pdata.perfdata, OTH, APPEND=YES);
%logparse(G:\dynasim\programs\run949\opt0\PVbpctabsAGE25.log, pdata.perfdata, OTH, APPEND=YES);
%logparse(G:\dynasim\programs\run949\opt0\PVbpctabsAGE65.log, pdata.perfdata, OTH, APPEND=YES);

%logparse(g:\dynasim\programs\run949\opt0\NtileByCohort.log, pdata.perfdata, OTH, APPEND=YES);                  /* tab assets */
%logparse(G:\dynasim\programs\run949\opt0\TaxTablesKarenBPCOPT0.log, pdata.perfdata, OTH, APPEND=YES);                  /* tab taxes */
%logparse(g:\dynasim\programs\run949\tabs\chpt8V3BaselineOPT0.log, pdata.perfdata, OTH, APPEND=YES);                    /* Chapter 8 tabs */
%logparse(G:\dynasim\programs\run949\tabs\FutretsecTab.log, pdata.perfdata, OTH, APPEND=YES);
%logparse(G:\dynasim\programs\run949\tabs\tabdisabKESV3.log, pdata.perfdata, OTH, APPEND=YES);

*%logparse(G:\dynasim\programs\run949\validation\RunValidationDynasim);              /* RUNS validation tool tabulations */
*%logparse(G:\dynasim\programs\run949\validation\RunValidationDynasimSHORTpt1);      /* copies sas output into validation excel sheets */
   
proc print data=pdata.perfdata;
var logfile stepname stepcnt realtime usertime systime obsin obsout varsout osmem ;
run;
     
   
   
   /* Add subsetting, analysis, and reporting here. */
  
 /* 
  * basement
   *%logparse( readfeh2090v4-SAS1.log, pdata.perfdata, OTH, APPEND=NO );
   *%logparse( MakeDependents-SAS1.log, pdata.perfdata, OTH, APPEND=YES );
   *%logparse( readfeh2090v4-SAS3.log, pdata.perfdata, OTH, APPEND=NO );
   *%logparse( MakeDependents-SAS3.log, pdata.perfdata, OTH, APPEND=YES );   
 */
 
 
  