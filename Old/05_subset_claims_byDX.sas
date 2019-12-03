/*******************************************************************************************************************/ 
/*	Purpose: 	
/*	Project: MACPAC Spending Variations
/*	Author: Leah Durbak
/*		1) Collapse macros for easier manipulation
/*	Notes: 
/*******************************************************************************************************************/ 
/*Options to change*/
%macro prod();
	options obs=MAX;
	/*Log
	proc printto print="P:\MCD-SPVR\log\subset_claims_byDX_&sysdate..lst"
	               log="P:\MCD-SPVR\log\subset_claims_byDX_&sysdate..log" NEW;
	run;*/
	proc printto;run;
%mend;

%macro test();	
	options obs=10000000;
	/*Log*/
	proc printto;run;
%mend;

%prod();
*%test();

libname  data    "P:\MCD-SPVR\data\raw_data\SAS_DATASETS";
libname  space   "P:\MCD-SPVR\data\workspace";
libname  area    "P:\MCD-SPVR\data\NO_PII";
libname  out     "P:\MCD-SPVR\data\workspace\output" COMPRESS=YES;
libname  library "P:\MCD-SPVR\data\workspace\output";
libname  stateot "P:\MCD-SPVR\data\raw_data\SAS_DATASETS\OT";
libname  cpds_wt "P:\MCD-SPVR\data\NO_PII\CDPS_WEIGHTS";
libname  scores  "P:\MCD-SPVR\data\workspace\CDPS_SCORES";

%let asth = "493", "4930", "49300", "49301", "49302", "4931", "49310", "49311", "49312", "4932", 
			"49320", "49321", "49322", "4938", "49381", "49382", "4939", "49390", "49391", "49392";
%let hypert = "401","4010","4011", "4019", "402", "4020", "40200", "4021", "40210", "4029", "40290", 
			"403", "4030", "40300", "4031", "40310", "4039", "40390", "404", "4040", "40400", "4041", "40410", 
			"4049", "40490", "405", "4050", "40501", "40509", "4051", "40511", "40519", "4059", "40591", "40599";
%let canc = "140", "1400", "1401", "1403", "1404", "1405", "1406", "1408", "1409", "141", "1410", "1411", 
			"1412", "1413", "1414", "1415", "1416", "1418", "1419", "142", "1420", "1421", "1422", "1428", "1429", 
			"143", "1430", "1431", "1438", "1439", "144", "1440", "1441", "1448", "1449", "145", "1450", "1451", 
			"1452", "1453", "1454", "1455", "1456", "1458", "1459", "146", "1460", "1461", "1462", "1463", "1464", 
			"1465", "1466", "1467", "1468", "1469", "147", "1470", "1471", "1472", "1473", "1478", "1479", "148", 
			"1480", "1481", "1482", "1483", "1488", "1489", "149", "1490", "1491", "1498", "1499", "160", "1600", 
			"1601", "1602", "1603", "1604", "1605", "1608", "1609", "161", "1610", "1611", "1612", "1613", "1618", 
			"1619", "172", "1720", "1721", "1722", "1723", "1724", "1725", "1726", "1727", "1728", "1729", "173", 
			"1730", "17300", "17301", "17302", "17309", "1731", "17310", "17311", "17312", "17319", "1732", "17320", 
			"17321", "17322", "17329", "1733", "17330", "17331", "17332", "17339", "1734", "17340", "17341", "17342", 
			"17349", "1735", "17350", "17351", "17352", "17359", "1736", "17360", "17361", "17362", "17369", "1737", 
			"17370", "17371", "17372", "17379", "1738", "17380", "17381", "17382", "17389", "1739", "17390", "17391", 
			"17392", "17399", "174", "1740", "1741", "1742", "1743", "1744", "1745", "1746", "1748", "1749", "175", 
			"1750", "1759", "179", "180", "1800", "1801", "1808", "1809", "181", "182", "1820", "1821", "1828", "184", 
			"1840", "1841", "1842", "1843", "1844", "1848", "1849", "185", "186", "1860", "1869", "187", "1871", "1872", 
			"1873", "1874", "1875", "1876", "1877", "1878", "1879", "188", "1880", "1881", "1882", "1883", "1884", "1885", 
			"1886", "1887", "1888", "1889", "189", "1890", "1891", "1892", "1893", "1894", "1898", "1899", "190", "1900", 
			"1901", "1902", "1903", "1904", "1905", "1906", "1907", "1908", "1909", "193", "195", "1950", "1951", "1952", 
			"1953", "1954", "1955", "1958", "V524";
%let psych = "290", "2900", "2901", "29010", "29011", "29012", "29013", "2902", "29020", "29021", "2903", "2904", 
			"29040", "29041", "29042", "29043", "2908", "2909", "301", "3010", "3011", "30110", "30111", "30112", "30113", 
			"3012", "30120", "30121", "30122", "3013", "3014", "3015", "30150", "30151", "30159", "3016", "3017", "3018", 
			"30181", "30182", "30184", "30189", "3019", "306", "3060", "3061", "3062", "3063", "3064", "3065", "30650", 
			"30651", "30652", "30653", "30659", "3066", "3067", "3068", "3069", "3071", "3075", "30750", "30751", "30752", 
			"30753", "30754", "30759", "3091", "3092", "30921", "30922", "30923", "30924", "30928", "30929", "3093", "3094", 
			"3098", "30982", "30983", "30989", "31081", "31089", "311", "3310", "3311", "3312";
%let preg = "630", "631", "6310", "6318", "632", "633", "6330", "63300", "63301", "6331", "63310", "63311", "6332", 
			"63320", "63321", "6338", "63380", "63381", "6339", "63390", "63391", "634", "6340", "63400", "63401", "63402", 
			"6341", "63410", "63411", "63412", "6342", "63420", "63421", "63422", "6343", "63430", "63431", "63432", "6344", 
			"63440", "63441", "63442", "6345", "63450", "63451", "63452", "6346", "63460", "63461", "63462", "6347", "63470", 
			"63471", "63472", "6348", "63480", "63481", "63482", "6349", "63490", "63491", "63492", "635", "6350", "63500", 
			"63501", "63502", "6351", "63510", "63511", "63512", "6352", "63520", "63521", "63522", "6353", "63530", "63531", 
			"63532", "6354", "63540", "63541", "63542", "6355", "63550", "63551", "63552", "6356", "63560", "63561", "63562", 
			"6357", "63570", "63571", "63572", "6358", "63580", "63581", "63582", "6359", "63590", "63591", "63592", "636", 
			"6360", "63600", "63601", "63602", "6361", "63610", "63611", "63612", "6362", "63620", "63621", "63622", "6363", 
			"63630", "63631", "63632", "6364", "63640", "63641", "63642", "6365", "63650", "63651", "63652", "6366", "63660", 
			"63661", "63662", "6367", "63670", "63671", "63672", "6368", "63680", "63681", "63682", "6369", "63690", "63691", 
			"63692", "637", "6370", "63700", "63701", "63702", "6371", "63710", "63711", "63712", "6372", "63720", "63721", 
			"63722", "6373", "63730", "63731", "63732", "6374", "63740", "63741", "63742", "6375", "63750", "63751", "63752", 
			"6376", "63760", "63761", "63762", "6377", "63770", "63771", "63772", "6378", "63780", "63781", "63782", "6379", 
			"63790", "63791", "63792", "638", "6380", "6381", "6382", "6383", "6384", "6385", "6386", "6387", "6388", "6389", 
			"639", "6390", "6391", "6392", "6393", "6394", "6395", "6396", "6398", "6399", "64001", "64081", "64091", "64101", 
			"64111", "64121", "64131", "64181", "64191", "64201", "64202", "64204", "64211", "64212", "64214", "64221", 
			"64222", "64224", "64231", "64232", "64234", "64241", "64242", "64244", "64251", "64252", "64254", "64261", 
			"64262", "64264", "64271", "64272", "64274", "64291", "64292", "64294", "64301", "64311", "64321", "64381", 
			"64391", "64421", "645", "64500", "64501", "6451", "64510", "64511", "6452", "64520", "64521", "6460", "64600", 
			"64601", "64611", "64612", "64614", "64621", "64622", "64624", "64631", "64641", "64642", "64644", "64651", 
			"64652", "64654", "64661", "64662", "64664", "64671", "64681", "64682", "64684", "64691", "64701", "64702", 
			"64704", "64711", "64712", "64714", "64721", "64722", "64724", "64731", "64732", "64734", "64741", "64742", 
			"64744", "64751", "64752", "64754", "64761", "64762", "64764", "64781", "64782", "64784", "64791", "64792", 
			"64794", "64801", "64802", "64804", "64811", "64812", "64814", "64821", "64822", "64824", "64831", "64832", 
			"64834", "64841", "64842", "64844", "64851", "64852", "64854", "64861", "64862", "64864", "64871", "64872", 
			"64874", "64881", "64882", "64884", "64891", "64892", "64894", "6498", "64981", "64982", "650", "651", "6510", 
			"65100", "65101", "6511", "65110", "65111", "6512", "65120", "65121", "6513", "65130", "65131", "6514", "65140", 
			"65141", "6515", "65150", "65151", "6516", "65160", "65161", "6517", "65170", "65171", "6518", "65180", "65181", 
			"6519", "65190", "65191", "652", "6520", "65200", "65201", "6521", "65210", "65211", "6522", "65220", "65221", 
			"6523", "65230", "65231", "6524", "65240", "65241", "6525", "65250", "65251", "6526", "65260", "65261", "6527", 
			"65270", "65271", "6528", "65280", "65281", "6529", "65290", "65291", "653", "6530", "65300", "65301", "6531", 
			"65310", "65311", "6532", "65320", "65321", "6533", "65330", "65331", "6534", "65340", "65341", "6535", "65350", 
			"65351", "6536", "65360", "65361", "6537", "65370", "65371", "6538", "65380", "65381", "6539", "65390", "65391", 
			"65401", "65402", "65404", "65411", "65412", "65414", "6542", "65420", "65421", "65431", "65432", "65434", 
			"65441", "65442", "65444", "65451", "65452", "65454", "65461", "65462", "65464", "65471", "65472", "65474", 
			"65481", "65482", "65484", "65491", "65492", "65494", "65501", "65511", "65521", "65531", "65541", "65551", 
			"65561", "65571", "65581", "65591", "65601", "65611", "65621", "65631", "65641", "65651", "65661", "65671", 
			"65681", "65691", "65701", "65801", "6581", "65810", "65811", "6582", "65820", "65821", "6583", "65830", "65831", 
			"6584", "65840", "65841", "6588", "65880", "65881", "6589", "65890", "65891", "659", "6590", "65900", "65901", 
			"6591", "65910", "65911", "6592", "65920", "65921", "6593", "65930", "65931", "6594", "65940", "65941", "6595", 
			"65950", "65951", "6596", "65960", "65961", "6597", "65970", "65971", "6598", "65980", "65981", "6599", "65990", 
			"65991", "660", "6600", "66000", "66001", "6601", "66010", "66011", "6602", "66020", "66021", "6603", "66030", 
			"66031", "6604", "66040", "66041", "6605", "66050", "66051", "6606", "66060", "66061", "6607", "66070", "66071", 
			"6608", "66080", "66081", "6609", "66090", "66091", "661", "6610", "66100", "66101", "6611", "66110", "66111", 
			"6612", "66120", "66121", "6613", "66130", "66131", "6614", "66140", "66141", "6619", "66190", "66191", "662", 
			"6620", "66200", "66201", "6621", "66210", "66211", "6622", "66220", "66221", "6623", "66230", "66231", "663", 
			"6630", "66300", "66301", "6631", "66310", "66311", "6632", "66320", "66321", "6633", "66330", "66331", "6634", 
			"66340", "66341", "6635", "66350", "66351", "6636", "66360", "66361", "6638", "66380", "66381", "6639", "66390", 
			"66391", "664", "6640", "66400", "66401", "66404", "6641", "66410", "66411", "66414", "6642", "66420", "66421", 
			"66424", "6643", "66430", "66431", "66434", "6644", "66440", "66441", "66444", "6645", "66450", "66451", "66454", 
			"66460", "66461", "66464", "6648", "66480", "66481", "66484", "6649", "66490", "66491", "66494", "665", "6650", 
			"66500", "66501", "6651", "66510", "66511", "6652", "66520", "66522", "66524", "6653", "66530", "66531", "66534", 
			"6654", "66540", "66541", "66544", "6655", "66550", "66551", "66554", "6656", "66560", "66561", "66564", "6657", 
			"66570", "66571", "66572", "66574", "6658", "66580", "66581", "66582", "66584", "6659", "66590", "66591", "66592", 
			"66594", "666", "6660", "66600", "66602", "66604", "6661", "66610", "66612", "66614", "6662", "66620", "66622", 
			"66624", "6663", "66630", "66632", "66634", "667", "6670", "66700", "66702", "66704", "6671", "66710", "66712", 
			"66714", "668", "6680", "66800", "66801", "66802", "66804", "6681", "66810", "66811", "66812", "66814", "6682", 
			"66820", "66821", "66822", "66824", "6688", "66880", "66881", "66882", "66884", "6689", "66890", "66891", "66892", 
			"66894", "669", "6690", "66900", "66901", "66902", "66904", "6691", "66910", "66911", "66912", "66914", "6692", 
			"66920", "66921", "66922", "66924", "6693", "66930", "66932", "66934", "6694", "66940", "66941", "66942", "66944", 
			"6695", "66950", "66951", "6696", "66960", "66961", "6697", "66970", "66971", "6698", "66980", "66981", "66982", 
			"66984", "6699", "66990", "66991", "66992", "66994", "670", "6700", "67000", "67002", "67004", "67010", "67012", 
			"67014", "67020", "67022", "67024", "67030", "67032", "67034", "67080", "67082", "67084", "671", "6710", "67100", 
			"67101", "67102", "67104", "6711", "67110", "67111", "67112", "67114", "6712", "67120", "67121", "67122", "67124", 
			"6713", "67130", "67131", "6714", "67140", "67142", "67144", "6715", "67150", "67151", "67152", "67154", "6718", 
			"67180", "67181", "67182", "67184", "6719", "67190", "67191", "67192", "67194", "672", "67200", "67202", "67204", 
			"673", "6730", "67300", "67301", "67302", "67304", "6731", "67310", "67311", "67312", "67314", "6732", "67320", 
			"67321", "67322", "67324", "6733", "67330", "67331", "67332", "67334", "6738", "67380", "67381", "67382", "67384", 
			"674", "6740", "67400", "67401", "67402", "67404", "6741", "67410", "67412", "67414", "6742", "67420", "67422", 
			"67424", "6743", "67430", "67432", "67434", "6744", "67440", "67442", "67444", "6745", "67450", "67451", "67452", 
			"67454", "6748", "67480", "67482", "67484", "6749", "67490", "67492", "67494", "675", "6750", "67500", "67501", 
			"67502", "67504", "6751", "67510", "67511", "67512", "67514", "6752", "67520", "67521", "67522", "67524", "6758", 
			"67580", "67581", "67582", "67584", "6759", "67590", "67591", "67592", "67594", "676", "6760", "67600", "67601", 
			"67602", "67604", "6761", "67610", "67611", "67612", "67614", "6762", "67620", "67621", "67622", "67624", "6763", 
			"67630", "67631", "67632", "67634", "6764", "67640", "67641", "67642", "67644", "6765", "67650", "67651", "67652", 
			"67654", "6766", "67660", "67661", "67662", "67664", "6768", "67680", "67681", "67682", "67684", "6769", "67690", 
			"67691", "67692", "67694", "677", "67800", "67801", "67810", "67811", "67900", "67901", "67902", "67904", "67910", 
			"67911", "67912", "67914", "V24", "V240", "V241", "V242", "V27", "V270", "V271", "V272", "V273", "V274", "V275", 
			"V276", "V277", "V279";
%let diab = "24900", "24901", "24980", "24981", "24990", "24991", "250", "2500", "25000", "25002", "2507", "25070", 
			"25071", "25072", "25073", "2508", "25080", "25081", "25082", "25083", "2509", "25090", "25091", "25092", 
			"25093", "3620", "36201", "36203", "36204", "36205";

%macro create_op_extract(indata, outdata);
	proc sql; 
		create table &outdata. as
		select BENE_ID, STATE_CD, DIAG_CD_1, MAX_TOS, PLC_OF_SRVC_CD, MDCD_PYMT_AMT, QTY_SRVC_UNITS, SRVC_BGN_DT, SRVC_END_DT,
			1 as CLM_CNT,
			SRVC_END_DT-SRVC_BGN_DT+1 as SRVC_DAYS,
			case when PLC_OF_SRVC_CD = 23 then "EMERG_ROOM"
				when MAX_TOS in (8,12) then "PHYS_CLIN"
				when MAX_TOS=11 then "HOSP_OPT"
				when MAX_TOS=16 then "RX_DRUG"
				when MAX_TOS=1 then "IP_HOSP"
			end as service_type,
			case when DIAG_CD_1 in (&asth.) then "ASTH"
				when DIAG_CD_1 in (&hypert.) then "HYPT"
				when DIAG_CD_1 in (&canc.) then "CANC"
				when DIAG_CD_1 in (&psych.) then "PSYC"
				when DIAG_CD_1 in (&preg.) then "PREG" 
				when DIAG_CD_1 in (&diab.) then "DIAB"
				end as svc_dx
		from &indata.
		where BENE_ID in (select distinct BENE_ID from space.id_pop_14feb2019) and TYPE_CLM_CD="1" and MAX_TOS in (1,8,12,11,16) or PLC_OF_SRVC_CD = 23 and 
			(DIAG_CD_1 in (&asth.) or DIAG_CD_1 in (&hypert.) or DIAG_CD_1 in (&canc.) or DIAG_CD_1 in (&psych.) or DIAG_CD_1 in (&preg.) or DIAG_CD_1 in (&diab.));
	quit;
%mend;

%macro create_ip_extract(indata, outdata);
	proc sql; 
		create table &outdata. as
		select BENE_ID, STATE_CD, DIAG_CD_1, MAX_TOS, MDCD_PYMT_AMT, SRVC_BGN_DT, SRVC_END_DT,
			1 as CLM_CNT,
			SRVC_END_DT-SRVC_BGN_DT+1 as SRVC_DAYS,
			"IP_HOSP" as service_type,
			case when DIAG_CD_1 in (&asth.) then "ASTH"
				when DIAG_CD_1 in (&hypert.) then "HYPT"
				when DIAG_CD_1 in (&canc.) then "CANC"
				when DIAG_CD_1 in (&psych.) then "PSYC"
				when DIAG_CD_1 in (&preg.) then "PREG" 
				when DIAG_CD_1 in (&diab.) then "DIAB"
				end as svc_dx
		from &indata.
		where BENE_ID in (select distinct BENE_ID from space.id_pop_14feb2019) and TYPE_CLM_CD="1" and MAX_TOS in (1,8,12,11,16) and 
			(DIAG_CD_1 in (&asth.) or DIAG_CD_1 in (&hypert.) or DIAG_CD_1 in (&canc.) or DIAG_CD_1 in (&psych.) or DIAG_CD_1 in (&preg.) or DIAG_CD_1 in (&diab.));
	quit;
%mend;

%macro create_sum_tab(state);
	%create_ip_extract(indata=data.Maxdata_ip_2012, outdata=ip_sub_&state.)
	%create_op_extract(indata=stateot.Maxdata_&state._ot_2012, outdata=ot_sub_&state.)

	proc sql;
		create table work.sum_&state. as
		(select
		  bene_id, state_cd, svc_dx, service_type, 
		  SUM(MDCD_PYMT_AMT) AS mdcd_pymt_amt,
		  SUM(QTY_SRVC_UNITS) AS QTY_SRVC_UNITS,
		  SUM(SRVC_DAYS) AS SRVC_DAYS,
		  SUM(CLM_CNT) AS CLM_CNT
		 from ot_sub_&state.
		 group by bene_id, state_cd, svc_dx, service_type)
	     union
		 (select
		  bene_id, state_cd, svc_dx, service_type, 
		  SUM(MDCD_PYMT_AMT) AS mdcd_pymt_amt,
		  SUM(SRVC_DAYS) AS SRVC_DAYS,
		  SUM(CLM_CNT) AS CLM_CNT
		  FROM work.ip_sub_&state.
		  GROUP BY bene_id, state_cd, svc_dx, service_type);
	QUIT;
%mend;

%macro do_states();
	%do i=1 %to 56;
		%if &i. ne 3 and &i. ne 7 and &i. ne 14 and &i. ne 43 and &i. ne 52 %then /*FIPS codes 3,7,14, 42, and 52 do not exist*/
			%do;
				%create_sum_tab(state = %sysfunc(fipstate(&i)) );
			%end;
	%end;
%mend;
%do_states();

/*CA, NY, TX did not run in above loop because of file naming*/
/*run these manually below*/
	%create_ip_extract(indata=data.Maxdata_ip_2012, outdata=ip_sub_ca);
	%create_op_extract(indata=stateot.Maxdata_ca_ot_2012_001, outdata=ot_sub_ca); /*these need to be different outdata and combine!*/
	%create_op_extract(indata=stateot.Maxdata_ca_ot_2012_002, outdata=ot_sub_ca);

	proc sql;
		create table work.sum_ca as
		(select
		  bene_id, state_cd, svc_dx, service_type, 
		  SUM(MDCD_PYMT_AMT) AS mdcd_pymt_amt,
		  SUM(QTY_SRVC_UNITS) AS QTY_SRVC_UNITS,
		  SUM(SRVC_DAYS) AS SRVC_DAYS,
		  SUM(CLM_CNT) AS CLM_CNT
		 from ot_sub_ca
		 group by bene_id, state_cd, svc_dx, service_type)
	     union
		 (select
		  bene_id, state_cd, svc_dx, service_type, 
		  SUM(MDCD_PYMT_AMT) AS mdcd_pymt_amt,
		  SUM(SRVC_DAYS) AS SRVC_DAYS,
		  SUM(CLM_CNT) AS CLM_CNT
		  FROM work.ip_sub_ca
		  GROUP BY bene_id, state_cd, svc_dx, service_type);
	QUIT;

	%create_ip_extract(indata=data.Maxdata_ip_2012, outdata=ip_sub_ny);
	%create_op_extract(indata=stateot.Maxdata_ny_ot_2012_001, outdata=ot_sub_ny);
	%create_op_extract(indata=stateot.Maxdata_ny_ot_2012_002, outdata=ot_sub_ny);

	proc sql;
		create table work.sum_ny as
		(select
		  bene_id, state_cd, svc_dx, service_type, 
		  SUM(MDCD_PYMT_AMT) AS mdcd_pymt_amt,
		  SUM(QTY_SRVC_UNITS) AS QTY_SRVC_UNITS,
		  SUM(SRVC_DAYS) AS SRVC_DAYS,
		  SUM(CLM_CNT) AS CLM_CNT
		 from ot_sub_ny
		 group by bene_id, state_cd, svc_dx, service_type)
	     union
		 (select
		  bene_id, state_cd, svc_dx, service_type, 
		  SUM(MDCD_PYMT_AMT) AS mdcd_pymt_amt,
		  SUM(SRVC_DAYS) AS SRVC_DAYS,
		  SUM(CLM_CNT) AS CLM_CNT
		  FROM work.ip_sub_ny
		  GROUP BY bene_id, state_cd, svc_dx, service_type);
	QUIT;

	%create_ip_extract(indata=data.Maxdata_ip_2012, outdata=ip_sub_tx);
	%create_op_extract(indata=stateot.Maxdata_tx_ot_2012_001, outdata=ot_sub_tx);
	%create_op_extract(indata=stateot.Maxdata_tx_ot_2012_002, outdata=ot_sub_tx);

	proc sql;
		create table work.sum_tx as
		(select
		  bene_id, state_cd, svc_dx, service_type, 
		  SUM(MDCD_PYMT_AMT) AS mdcd_pymt_amt,
		  SUM(QTY_SRVC_UNITS) AS QTY_SRVC_UNITS,
		  SUM(SRVC_DAYS) AS SRVC_DAYS,
		  SUM(CLM_CNT) AS CLM_CNT
		 from ot_sub_tx
		 group by bene_id, state_cd, svc_dx, service_type)
	     union
		 (select
		  bene_id, state_cd, svc_dx, service_type, 
		  SUM(MDCD_PYMT_AMT) AS mdcd_pymt_amt,
		  SUM(SRVC_DAYS) AS SRVC_DAYS,
		  SUM(CLM_CNT) AS CLM_CNT
		  FROM work.ip_sub_tx
		  GROUP BY bene_id, state_cd, svc_dx, service_type);
	QUIT;

data space.max_cdpsclaims;
	set sum_:;
	recipno=catx ('_',STATE_CD,BENE_ID); 
run;

/*create counts of nursing facility claims*/
proc sql; /*these need to be summed for bene_id*/
/*should able to appended these rows to IP/OP data*/
	create table count_lt as
	select bene_id, MSIS_TOS as lt_MSIS_TOS, MDCD_PYMT_AMT,
				case when DIAG_CD_1 in (&asth.) then "ASTH"
				when DIAG_CD_1 or DIAG_CD_2 in (&hypert.) then "HYPT"
				when DIAG_CD_1 or DIAG_CD_2 in (&canc.) then "CANC"
				when DIAG_CD_1 or DIAG_CD_2 in (&psych.) then "PSYC"
				when DIAG_CD_1 or DIAG_CD_2 in (&preg.) then "PREG" 
				when DIAG_CD_1 or DIAG_CD_2 in (&diab.) then "DIAB"
				end as svc_dx
	from data.maxdata_lt_2012 /*take out diangosis code 2*/
	where MSIS_TOS = 7 and (DIAG_CD_1 in (&asth, &hypert, &canc, &psych, &preg, &diab) or DIAG_CD_2 in (&asth, &hypert, &canc, &psych, &preg, &diab));

proc sql;
	create table max_lt_cdps as
	select a.*, b.lt_MSIS_TOS, b.MDCD_PYMT_AMT as MDCD_PYMT_AMT_lt, b.svc_dx as svc_dx_lt
	from max2012_cdps_subset_addps a left join count_lt b
	on a.bene_id = b.bene_id;
quit;

/* put all the dataset names in temp library into macro variable &names*/
/*
proc sql noprint;
	select catx('.', "data_ot",memname) into :names separated by '(where=(MSIS_TOS=11) '
	from dictionary.tables 
	where libname=upcase('data_ot') and memname like upcase('%_OT_2012%');
quit;

/*this takes a long time bc the OT files are many and massive*
data space.max_ot_2012_tos11;
  set &names;
run;
*/
/*create counts of outpatient hospital claims */
/*this is duplcated of above
proc sql;
	create table count_ot as
	select bene_id, MSIS_TOS as MSIS_TOS_ot11, MDCD_PYMT_AMT as MDCD_PYMT_AMT_ot11
	from space.max_ot_2012_tos11 
	where (DIAG_CD_1 in (&asth, &hypert, &canc, &psych, &preg, &diab) or DIAG_CD_2 in (&asth, &hypert, &canc, &psych, &preg, &diab));

	create table max_cdps_lt_ot as
	select a.*, b.MSIS_TOS_ot11, b.MDCD_PYMT_AMT_ot11
	from max_lt_cdps a left join count_ot b
	on a.bene_id = b.bene_id;
quit;
*/
/*bring in max ps data for additional vars*/
/*i don't think this needs to happen, or needs to be at the end to avoid dupes*/
proc sql; 
	create table max2012_cdps_subset_addps as
	select a. *, 
		b.TOT_IP_STAY_CNT, 
		b.TOT_IP_DAY_CNT_STAYS, 
		b.FFS_PYMT_AMT_01, 
		(b.FFS_CLM_CNT_08 + b.FFS_CLM_CNT_12) as phys_clin_claims, 
		(b.FFS_PYMT_AMT_08 + b.FFS_PYMT_AMT_12) as phys_clin_spending, 
		b.FFS_CLM_CNT_16, 
		b.FFS_PYMT_AMT_16 /*add ffs_clm_amt_11 & ff_clm_amt_07 for ot spending*/
	from space.max_cdpsclaims a left join data.maxdata_ps_2012 b /*across all service tpyes/dx*/
	on a.bene_id = b.bene_id;
quit;

/*limit data and add previously-calculated CDPS score*/
proc sql;
	create table space.max2012_cdps_subset_wltot as
	select *, substr(age_servicetype, length(age_servicetype)-1,2) as servicetype
	from max_cdps_lt_ot a left join space.max_cdpsscores b
	on a.bene_id = b.bene_id
	where (dia2l = 1 or carel = 1 or canl = 1 or prgcmp = 1 or psyl = 1 or pula = 1) and calculated servicetype in ("05","06","07","08");
quit; 

