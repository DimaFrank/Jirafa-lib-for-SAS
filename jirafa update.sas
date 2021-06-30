

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
		
		select &column_name., count(&column_name.) as freq
		from &libname..&table_name.
		group by &column_name.
		order by 2 DESC

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
	
		select &column_name.
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
	
		select &column_name.
	  	from &libname..&table_name. 
		order by &column_name. DESC	
	;
	quit;


%mend nlargest;



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




%macro clean(table_name);


	%let libname = WORK;
	
	proc delete data = &libname..&table_name. ;
	run;


%mend clean;



%macro to_csv(table_name, path);

	%let libname = WORK;

	proc export
  		data= &libname..&table_name.
  		dbms=csv 
  		outfile = &path. 
  		replace;
	run;


%mend to_csv;



%macro to_excel(table_name, path);

	%let libname = WORK;

	proc export
  		data= &libname..&table_name.
  		dbms=xlsx 
  		outfile = &path. 
  		replace;
	run;



%mend to_excel;



%macro random(n);

data Rand;
	call streaminit(123);       
   do i = 1 to &n.;
   u = rand("Uniform");     /* u ~ U(0,1) */
   output;
end;
run;



%mend random;





%macro loc(table_name, column_name1);

	%let libname = WORK;

	proc sql;
	
	create table &table_name._&column_name1.
	as
	select &column_name1.
	from &libname..&table_name.

	;
	quit;


%mend loc;



%macro mean(table_name, column_name);

	%let libname = WORK;

	proc sql;

	create table &table_name._mean_&column_name.
	as
	select mean(&column_name.)
	from &libname..&table_name.

	;
	quit;


%mend mean;



%macro median(table_name, column_name);

	%let libname = WORK;

	proc sql;

	create table &table_name._median_&column_name.
	as
	select median(&column_name.)
	from &libname..&table_name.

	;
	quit;


%mend median;



%macro sum(table_name, column_name);

	%let libname = WORK;

	proc sql;

	create table &table_name._sum_&column_name.
	as
	select sum(&column_name.)
	from &libname..&table_name.

	;
	quit;


%mend sum;




%macro append(table_name1, table_name2);

	%let libname = WORK;

	proc sql;

	create table &table_name1._append_&table_name2.
	as
	select *
	from &libname..&table_name1.

	Union

	select *
	from &libname..&table_name2.

	;
	quit;


%mend append;



%macro insert_column(table_name, table_name_to_integrate, col_name_to_integrate);

	%let libname = WORK;

/*
	%shape(&table_name.)
	%shape(&table_name_to_integrate.)

	
	proc sql;
	
		select number_of_rows into: &x  
		from &libname..&table_name._shape
		;quit;
 
	proc sql;

		select number_of_rows into: &y
		from &libname..&table_name_to_integrate._shape
		;quit;
	
*/


	proc sql noprint;
 	create table &table_name.
	as

    select 0 as ind, *
	from &libname..&table_name.


	;
	quit;
	


	proc sql noprint;
 	create table &table_name_to_integrate.
	as

    select 0 as ind, *
	from &libname..&table_name_to_integrate.


	;
	quit;
	


	proc sort data =  &table_name.;
	by ind;
	run;

	data &table_name._ranked;
	set &libname..&table_name.;
	by ind;
	if first.id then rank =1;
	else rank+1;
	run;


	proc sort data = &table_name_to_integrate.;
	by ind;
	run;

	data &table_name_to_integrate._ranked;
	set &libname..&table_name_to_integrate.;
	by ind;
	if first.id then rank =1;
	else rank+1;
	run;


	proc sql;
	create table &table_name._inserted
	as

	select tab1.*, tab2.&col_name_to_integrate.
	from &libname..&table_name._ranked as tab1
	left join &libname..&table_name_to_integrate._ranked as tab2
	on tab1.rank = tab2.rank

	;
	quit;


	%clean(&table_name.)
	%clean(&table_name_to_integrate.)
	%clean(&table_name._ranked)
	%clean(&table_name_to_integrate._ranked)
	%drop_column(&table_name._inserted, ind)
	%drop_column(&table_name._inserted, rank)
	



%mend insert_column;




%macro drop_column(table_name, column_name_to_drop);

	%let libname = WORK;

	
	data &table_name.;
		set &libname..&table_name. (drop = &column_name_to_drop.);

	run;
	

%mend drop_column;





/*
%macro isna(table_name, column_name);

	%let libname = WORK;

	proc sql;
	create table &table_name._isna
	as


	SELECT count(missings)
	FROM(
		select case 
					when &column_name. in (" ") then NULL 
					else &column_name.
					end as missings

		from &libname..&table_name.
		)
    WHERE missings = NULL

	;
	quit;



%mend isna;
*/



