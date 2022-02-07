%macro getdatafromdatabricks(in_query/*Query based on which data should be extracted from Databricks*/,in_libname/*Name of the lib to use*/, in_libstatement/*Libstatement*/,in_tablename/*Table name where the data should land*/ );

/*define constants for the integration*/

/*Define folder for temp file and and name for temp file*/
%let tmpdirectory=/opt/sas/sasdata;
%let tmpfilename= dbrtmpfile%sysfunc(compress(%sysfunc(DATETIME()),".")).csv;


/*dbr access_token eg dapixxxxxxxxxxxxxxxxxxxxx*/
%let access_token=dapi23213213123213123123123;
/*url to dbr instance eg. https://xxxxxxxxxxxx.1.azuredatabricks.net*/
%let dbr_url=https://xxxxxxxxxxx.1.azuredatabricks.net; 

/*Set job_id from databricks*/
%let job_id=292;

/*waiting for dbr job to be completed vars*/
%let max_attemps_wait=100;
%let attemps_timeout=10000;



/*set options*/
option sastrace=',,,d' sastraceloc=saslog nostsuffix msglevel=i;
option set=work="/opt/sas/sasdata";

/*define variable*/
%let run_id=;
%let life_cycle_state=;
%let result_state=;


/*set libname based on defined inputy*/
LIBNAME &in_libname. &in_libstatement.;

/*define temp file to store and process http responses*/
filename respfile "/opt/sas/sasdata/outhttprespfile";


%let json_rq=%tslit(%str({"job_id": &job_id., "notebook_params": {"query": &in_query.,"tmpfilename":"&tmpfilename..gz" }}));

/*run the job that extracts and transfers data in dbr*/
proc http
url="&dbr_url/api/2.1/jobs/run-now"
method="POST"
in=&json_rq
out=respfile;
headers "Authorization"="Bearer &access_token";
run;

/* Using JSON engine prepare http response file to be parsed */
libname resplib JSON fileref=respfile;

/* Parse response file to extract run_id of the called job*/
proc sql noprint;
   select run_id
      into :run_id
      from resplib.ROOT
where ordinal_root=1;


%put "The job has been submitted the assigned run_id=&run_id.";


%macro waittilljobiscompleted;
%local i;
/*do five attemps to validate if the job is completed*/
%do i=1 %to &max_attemps_wait.;

	proc http
	url="&dbr_url/api/2.1/jobs/runs/get?run_id=&run_id"
	method="GET"
	out=respfile;
	headers "Authorization"="Bearer &access_token.";
	run;

	libname resplib JSON fileref=respfile;


    /*get life_cycle_state from the respone*/
	proc sql noprint;
	   select life_cycle_state
	      into :life_cycle_state
	      from resplib.STATE
	where ordinal_root=1;
	%put &life_cycle_state;

/*repeat untill life_cycle_state=TERMINATED*/
%IF &life_cycle_state=TERMINATED %THEN
%do; 
	/*get result_state*/
		proc sql noprint;
		   select result_state
		      into :result_state
		      from resplib.STATE
		where ordinal_root=1;
	
		%goto continue;
%end;

/*sleep untill retry*/
data _null_;
   call sleep(&attemps_timeout.);
run;     
 
%end;
%continue:
%mend waittilljobiscompleted;

%waittilljobiscompleted;



/*decomress file using linux gzip utility. Requires Allow XCMD option be set to True */
%sysexec %str(gzip -d /opt/sas/sasdata/&tmpfilename..gz);


/*import csv file to the target dest*/
proc import datafile="&tmpdirectory./&tmpfilename."
            dbms=csv
            out=&in_libname..&in_tablename.
            replace;
run;

%mend getdatafromdatabricks;

/* Example on how macro can be called*/
%getdatafromdatabricks("select * from default.sales_history;", test, BASE "/opt/sas/sasdata",drakon4);
