# Logparse
SAS Log parse utilities

Original work by SAS support , downloaded from http://support.sas.com/kb/34/301.html and adapted. This is a good summary of how Logparse.sas works and how to adapt it: http://www2.sas.com/proceedings/sugi30/219-30.pdf


I've removed many of the variables which were not populating from the logs we have, but have kept: logfile, stepname, realtime, usertime, cputime, obsin, obsout, varsout, osmem, and stepcnt. Actually did not remove them, just dropped from the final output.

Logparse.sas is a macro which should be included in a SAS program, example provided is readlog.sas, which currently reads all logs for a Dynasim postprocessor run.

A libname for the resulting SAS dataset needs to be defined in the program, example here is 'pdata' (for "performance data').

When run in SAS Studio, the HTML output can be viewed in the browser, and optionally you can open the html in Excel and do some calculations on the run times for the steps in your SAS programs. Example xls file included here.

Also will upload a .py script which does a simple search and output of interesting strings in SAS logs, a user can customize easily by substituting strings (substring1, substring2, ..., substringN)

Note to Dynasim users on SAS1 - there is a logparse directory "G:\Dynasim\Logparse" where the SAS part is already in operation.

Doug Murray 1-30-2018
