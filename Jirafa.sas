/**********************************************************************************************************************
*********************************************************************************************************************** 
*********************************************************************************************************************** 
Jirafa library created in order to make SAS data exploration easier						   
and to provide a number of SAS macro language quick solutions for most common and useful actions. 							                                      
																													  
Last Update: 24/06/2021																							      
*************************************************************************************************************************
*************************************************************************************************************************
*************************************************************************************************************************
*/



%macro head(table_name, n);

	%let lib = WORK;

	%IF &n > 0 %THEN %DO; 

		DATA &lib..&table_name.;
			set &lib..&table_name. (OBS = &n.);

/*		PROC PRINT DATA = &lib..&table_name. (OBS = &n);*/
		RUN;
    %END;

	%ELSE %DO;

	 	%PUT WARNING: The value of n shoud be greater then 0.;
	%END;
		

%mend head;





%macro tail(table_name, n);

	%let lib = WORK;

	data &table_name._tail;
	  do p = max(1,nobs-&n.+1) to nobs;
	  set &lib..&table_name. nobs=nobs point=p;
	   output;
	   end;
	   stop;

	run;

%mend tail;




%macro shape(table_name);

	%let lib = WORK;
	%let rows_num = 0;
	
	proc sql noprint;
		create table &table_name._shape 
		as
		select Count(*) as number_of_rows
		FROM &lib..&table_name.
	;quit;
	/*
	data result;
		number_of_rows = &rows_num.;
    run;

	proc print data=result; 
	run;
*/
%mend shape;






%macro info(table_name);

	proc freq data=work.&table_name.;
	    table _all_ / missing;
	run;

	/* create a format to group missing and nonmissing */
/*
	proc format;
		 value $missfmt ' '='Missing' other='Not Missing';
		 value  missfmt  . ='Missing' other='Not Missing';
	run;
	 
	proc freq data=&table_name.; 


		format _CHAR_ $missfmt.; /* apply format for the duration of this PROC */
/*		tables _CHAR_ / missing missprint nocum nopercent;
		format _NUMERIC_ missfmt.;
		tables _NUMERIC_ / missing missprint nocum nopercent;
	run;
*/

%mend info;




%macro sort(table_name, by_column_number, order_type);

	%let lib = WORK;
	

	%IF &order_type = DESC %THEN %DO;
		%LET order = DESC;
	%END;


	%ELSE %IF &order_type = ASC %THEN %DO;
		%LET order = ASC;
	%END;


	%ELSE %DO;
		%PUT WARNING: The order_type attribute shoud be 0 or 1 .;
	 %END;
		

		proc sql;
		create table &table_name._sorted
		as
		select *
		from &lib..&table_name.
		order by &by_column_number. &order.

		
%mend sort;



%macro convert_to_num(varname,library,dataset);

proc sql noprint;
select type into :vartype
from dictionary.columns
where libname = upcase("&library.") and memname = upcase("&dataset.") and upcase(name) = upcase("&varname.")
;
quit;

%if &vartype. = char
%then %do;

data &library..&dataset. (drop=__&varname.);
set &library..&dataset. (rename=(&varname.=__&varname.));
&varname. = input(__&varname.,best.);
run;

%end;

%mend convert_to_num;


%macro convert_to_char(table_name, column_name);

	%let libname = WORK;

	data &libname..&table_name._converted ;
		set &libname..&table_name.;
		&column_name._converted = PUT(&column_name.,best. );
	run;


%mend convert_to_char;





%macro describe(dataset, varname);
	
	%LET lib = WORK;

		
		proc means data=&lib..&dataset. Mean Median Mode P25 P50 P75;
			var &varname.;
		run;

/*

	%IF (vartype(&varname.) = C) %THEN %DO;

		proc freq data=&lib..&dataset.;
			tables &varname.;
		run;
	%END;

*/
 
%mend describe;


%macro read_csv(path, headers);

	proc import datafile = &path.
		out= outdata
		dbms = csv
		replace;

		%IF &headers. = 'True' %THEN %DO;
			getnames = yes;
		%END;
		
		%IF &headers. = 'False' %THEN %DO;
			getnames = no;
		%END;
	
	run;

%mend read_csv;


%macro read_excel(path, headers);

	proc import datafile = &path.
		out= outdata
		dbms = xlsx
		replace;

		%IF &headers. = 'True' %THEN %DO;
			getnames = yes;
		%END;
		
		%IF &headers. = 'False' %THEN %DO;
			getnames = no;
		%END;
	
	run;

%mend read_excel;



%macro groupby(table_name, group_by_column, aggregate_funct, aggregate_column);

	%let libname = WORK;

	 proc sql;
	 	create table &table_name._grouped
		as
	 	select &group_by_column., &aggregate_funct.(&aggregate_column.)
		from &libname..&table_name.
		group by &group_by_column.
	;
	quit;


%mend groupby;



%macro value_counts(table_name, column_name);

	%let libname = WORK;

	proc sql;
		create table &table_name._valueCounts
		as
		
		select &column_name., count(&column_name.)
		from &libname..&table_name.
		group by &column_name.

	;
	quit;

%mend value_counts;



%macro nunique(table_name, column_name);
	
	%let libname = WORK;
	proc sql;
	    create table &table_name._nunique
		as

	 	select count(distinct(&column_name.)) as &column_name._unigue_value
	 	from &libname..&table_name.
	;
	quit;
%mend nunique;



		

%macro slice(table_name,start_row, end_row);
	
	%let libname = WORK;

	data &libname..&table_name._sliced;

		set &libname..&table_name.(firstobs = &start_row. obs = &end_row.);
	run; 

%mend slice;


%macro nsmallest(table_name, column_name, n);

	%let libname = WORK;

	proc sql outobs = &n.;
  		create table &table_name._nsmallest
  		as 
	
		select *
	  	from &libname..&table_name. 
		order by &column_name. ASC	
	;
	quit;


%mend nsmallest;


%macro nlargest(table_name, column_name, n);

	%let libname = WORK;

	proc sql outobs = &n.;
  		create table &table_name._nlargest
  		as 
	
		select *
	  	from &libname..&table_name. 
		order by &column_name. DESC	
	;
	quit;


%mend nlargest;(




%macro merge( left_table, how, right_table, on_left_index, on_rigth_index);


		proc sql;
			create table &left_table._merge_&right_table.
			as
			select *
			from &left_table. 
			&how. join &right_table.
			on &left_table..&on_left_index. = &right_table..&on_rigth_index.
		;
		quit;
 

%mend merge;



%macro rename_table(old, new);
	
	%let libname = WORK;

	proc sql;
	create table &new.
	as
	select *
	from &libname..&old.
	;
	quit;

	proc sql;
		drop table &libname..&old.
	;
	quit;

%mend rename_table;





%macro rename_column(dataset, old_column_name, new_column_name);

	%let libname = WORK;

	data &dataset.;
		set &libname..&dataset.;
		rename &old_column_name. = &new_column_name.;
	run;


%mend rename_column;