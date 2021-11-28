/**************************************************************************************************************************
************************************************************************************************************************* *
*********************************************************************************************************************** * *
Jirafa library created in order to make SAS data exploration easier						      * * *
and to provide a number of SAS macro language quick solutions for most common and useful actions. 		      * * *
Created by Dima 22/06/2021											      * * *
														      * * *
														      * * *
Last Update: 1/11/2021										                      * * *
*********************************************************************************************************************** * *
************************************************************************************************************************* *
***************************************************************************************************************************/






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

	%col_names(&lib., &table_name.);

	proc sql noprint;
		create table &table_name._shape 
		as

		select Count(*) as number_of_rows, (select count(*) from columns_of_&table_name.) as number_of_columns
		FROM &lib..&table_name.
	;
	quit;

	%clean(columns_of_&table_name.);


%mend shape;





%macro info(table_name);

	proc freq data=work.&table_name.;
	    table _all_ / missing;
	run;


%mend info;




%macro sort(table_name, by_column_number, order_type);

	%let lib = WORK;
	

	%IF &order_type = DESC | &order_type = desc | &order_type = Desc
	%THEN %DO;
		%LET order = DESC;
	%END;


	%ELSE %IF &order_type = ASC | &order_type = asc | &order_type = Asc 
	%THEN %DO;
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

	data &libname..&table_name._converted;
		set &libname..&table_name.;
		&column_name._converted = PUT(&column_name.,best. );
	run;

	%drop_column(&table_name._converted , &column_name.);
	%rename_column(&table_name._converted , &column_name._converted , &column_name.);
	
	
	proc sql;
	create table &table_name.
	as
	select &column_name. , *
	from &libname..&table_name._converted
	;
	quit;

	%clean(&table_name._converted);

%mend convert_to_char;





%macro describe(dataset, varname);
	
	%LET lib = WORK;

		
		proc means data=&lib..&dataset. Mean Median Mode P25 P50 P75;
			var &varname.;
		run;

 
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




%macro slice(table_name, start_row, end_row, step=1);


%let libname = WORK;


	  %IF (&step. <= 0) OR (&start_row. <= 0) OR(&end_row. <= 0)	 	
		  %THEN  %DO;

			%put ERROR: Negative or zero values are not allowed;

		  %END;

	  %ELSE %IF (&end_row. < &start_row.) 
	     %THEN %DO;

			%put ERROR: The end_row is greater then the start_row;

		 %END;

	%IF &step. > 1 %THEN
		%DO;

			data temp1;
				set &libname..&table_name.(firstobs = &start_row. obs = &end_row.);
			run; 

			data temp2;
				set temp1;
			    rownum=_n_;
			    ind = MOD(rownum,&step.);
			run;

			proc sql;
				create table &table_name._sliced
			 	as
			 	select *
			 	from temp2
			 	where ind = 1
			;
			quit;

			%drop_column(&table_name._sliced, ind);
			%drop_column(&table_name._sliced, rownum);

	   %END;

     %ELSE %DO;

			data &table_name._sliced;

				set &libname..&table_name.(firstobs = &start_row. obs = &end_row.);
			run; 

	    %END;

	%clean(outdata);
	%clean(temp1);
	%clean(temp2);



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




%macro rename_table(old_name, new_name);

	%let libname = WORK;

	proc sql;
	create table &new_name.
	as
	select *
	from &libname..&old_name.
	;
	quit;

	proc sql;
		drop table &libname..&old_name.
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




%macro iloc(table_name, i, j);

	%let libname = WORK;
	%shape(&table_name);
	
	proc sql noprint;
	select number_of_rows, number_of_columns
	into: num_of_rows, :num_of_cols
	from &table_name._shape
	;
	quit;

	%IF &i. > &num_of_rows. OR &j. > &num_of_cols.
		%THEN %DO;
			%put WARNING: One of the (i,j) arguments is out of range!;

		%END;

	%ELSE; 
		%DO;

			proc sql;
				create table cols_order
				as

				select upper(trim(name)) as name, varnum
				from dictionary.columns
				where upcase(libname) = "&libname."
				and memname = upcase("&table_name.")
			;
			quit;
			
			data rows_rank;
					set &table_name.;
					rownum = _n_;
				run;
			

			proc sql noprint;
				select lower(name) 
				into: col_name
				from cols_order 
				where varnum = &j.
			;
			quit;


			proc sql;
			create table &table_name._iloc
			as
				select &col_name.
				from rows_rank
				where rownum = &i.
			;
			quit;

		%END;

		%clean(cols_order);
		%clean(rows_rank);
		%clean(&table_name._shape);


%mend iloc;




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


	%clean(&table_name._ranked)
	%clean(&table_name_to_integrate._ranked)
	%drop_column(&table_name._inserted, ind)
	%drop_column(&table_name._inserted, rank)
	%drop_column(&table_name., ind);
	%drop_column(&table_name_to_integrate., ind );
	



%mend insert_column;




%macro drop_column(table_name, column_name_to_drop);

	%let libname = WORK;

	
	data &table_name.;
		set &libname..&table_name. (drop = &column_name_to_drop.);

	run;
	

%mend drop_column;


/*Reverse char values in given column*/
%macro reverse_column(table_name, column_name, drop_orig);
	
	%let libname = WORK;

	proc sql;
	create table &table_name.
	as
	select *, Reverse(&column_name.) as &column_name._reversed
	from &libname..&table_name.
	
	;
	quit;

	%IF &drop_orig. = True | &drop_orig. = TRUE | &drop_orig. = true
	%THEN %DO;
	%drop_column(&table_name., &column_name.);

	%END;


%mend reverse_column;

%macro col_names(lib_name, table_name);
	
	proc sql;
		create table columns_of_&table_name.
		as

		select name, type
		from dictionary.columns
		where libname = upcase("&lib_name.")
			and memname = upcase("&table_name.")
	;
	quit;
	;
	

%mend col_names;




%macro get_all_numeric(lib_name, table_name);
	
	proc sql;
		create table all_numeric_&table_name.
		as

		select name
		from dictionary.columns
		where libname = upcase("&lib_name.")
			and memname = upcase("&table_name.")
			and type = 'num'
	;
	quit;
	;
	


%mend get_all_numeric;




%macro get_all_chars(lib_name, table_name);

	proc sql;
		create table all_chars_&table_name.
		as

		select name

		from dictionary.columns
		where libname = upcase("&lib_name.")
			and memname = upcase("&table_name.")
			and type = 'char'
	;
	quit;
	;


%mend get_all_chars;




%macro desc(lib_name, table_name);

	proc sql;
		create table &table_name._desc
		as

		select name, type, length, format

		from dictionary.columns
		where libname = upcase("&lib_name.") 
			and memname = upcase("&table_name.")
	
	;
	quit;
	;


%mend desc;




%macro appear(table_name, col_name, value);

	%let libname = WORK;
	%col_names(&libname. , &table_name.);


	
	proc sql;
	create table temp
	as
	SELECT &col_name.,
						case 
							when &value. = trim(&col_name.)
							then 1 
							else 0
							end as ind

	FROM &libname..&table_name.

	;
	quit;

	
	proc sql;
	select 
		   case when SUM(ind)> 0 then "True"
		   else "False"
		   end as result
    from temp
	;
	quit;

	%clean(columns_of_&table_name.);
	%clean(temp);

	

%mend appear;





/*Creates vector of random integers in specific range*/
%macro rand(n, range_start, range_end);

	data RandInt;
	do i = 1 to &n.;
		x = rand("Integer", &range_start., &range_end.);
		output;
	end;
	run;

%mend rand;
	



%macro clone(table_name);

	%let libname = WORK;
	proc sql;
		create table Copy_of_&table_name.
		as

		select *
		from &libname..&table_name.
		;
		quit;

%mend clone;




%macro replace(table_name, column_name, value_to_replace, replace_by);

	%let libname = WORk;

	proc sql;
		create table &table_name.
		as

		select *,
				 case
				 	 when &column_name. = &value_to_replace.
					 	 then &replace_by.
				     else 
					 	 &column_name.

					 end as &column_name._new


		from &libname..&table_name.
		;
		quit;


		%drop_column(&table_name., &column_name.);
		%rename_column(&table_name.,&column_name._new,&column_name.);


%mend replace;





%macro convert_numeric_date(dataset, variable);

data &dataset.;
	set &dataset.;
	my_date_char = put(&variable., 8.);
	sas_date_value = input(my_date_char, yymmdd8.);
	sas_date_format = sas_date_value;
 
    format sas_date_format yymmdd10.;

run;

%drop_column(&dataset., sas_date_value);
%drop_column(&dataset., my_date_char);
%drop_column(&dataset., &variable.);
%rename_column(&dataset., sas_date_format, &variable.);


%mend convert_numeric_date;





%macro swap_columns(table_name, column1, column2);


%let column1 = %upcase(%trim(&column1));
%let column2 = %upcase(%trim(&column2));
%let libname = WORK;

	proc sql;
		create table cols_order
		as

		select upper(trim(name)) as name, varnum
		from dictionary.columns
		where upcase(libname) = "&libname."
		and memname = upcase("&table_name.")
	;
	quit;

	proc sql;
		create table temp
		as
		select *, case when name = "&column1." then "&column2."
					   when name = "&column2." then "&column1."
					   else name
				  end as name_new


		from cols_order
	;
	quit;

	%drop_column(temp, name);
	%rename_column(temp, name_new, name);

	proc sql;
		create table new_order
		as
		select name, varnum
		from temp 

		UNION 
		select *
		from cols_order
		where name not in (select name from temp)
		order by 2
	;
	quit;

	%clean(cols_order);
	%clean(temp);

	%global List;
	proc sql noprint;
		select name
		into: List separated by ","
		from new_order
	;
	quit;


	proc sql;
		create table &table_name.
		as
		select &List.
		from &table_name.
	;
	quit;

	%clean(new_order);



%mend swap_columns;



options minoperator mlogic;
%macro isna(table_name)/mindelimiter=',';

	%let libname = WORK;

	%shape(&table_name.);
	%col_names(&libname., &table_name.);
	%get_all_chars(&libname., &table_name.);
	%get_all_numeric(&libname., &table_name.);
	%clone(&table_name);
	%rename_table(copy_of_&table_name., temp);
	


	%global List_of_num_cols;
	proc sql noprint;
		select upper(name)
		into: List_of_num_cols separated by ","
		from all_numeric_&table_name.
	;
	quit;

	%global List_of_char_cols;
	proc sql noprint;
		select upper(name)
		into: List_of_char_cols separated by ","
		from all_chars_&table_name.
	;
	quit;



	proc sql noprint;
		select number_of_columns
		into: num_of_cols
		from &table_name._shape
	;
	quit;


	proc sql;
		create table cols_order
		as

		select upper(trim(name)) as name, varnum
		from dictionary.columns
		where upcase(libname) = "&libname."
		and memname = upcase("&table_name.")

	;
	quit;
	
	%DO i=1 %TO &num_of_cols;

			proc sql noprint;
				select trim(name)
				into: col_name
				from cols_order
				where varnum = &i
			;
			quit;	
				

			
			%IF &col_name in (&List_of_num_cols) 
					%THEN %DO;
						proc sql;
						create table temp
						as
						select *,
								 case 
								 	  when &col_name = .
									  	  then 'True'
									  else
									  	  'False'
								end as _&col_name.

						from temp
						;quit;
					%END;


			%ELSE 
				%IF &col_name in (&List_of_char_cols) 
					%THEN %DO;
						proc sql;
						create table temp
						as
						select *,
								case 
								 	  when &col_name = ''
									  	  then 'True'
									  else
									  	  'False'
								end as _&col_name.

						from temp
						;quit;

				%END;
	

	%END;



	%col_names(&libname., temp);

	%global new_columns;
		proc sql noprint;
			select name
			into: new_columns separated by ","
			from columns_of_temp
			where substr(name,1,1) in('_')
		;
		quit;

	proc sql;
		createt table &table_name._isna
		as
		select &new_columns.
		from temp
	;
	quit;

	%clean(&table_name._shape);
	%clean(columns_of_&table_name.);
	%clean(all_chars_&table_name.);
	%clean(all_numeric_&table_name.);
	%clean(temp);
	%clean(cols_order);
	%clean(columns_of_temp);



	%col_names(&libname., &table_name._isna);
	data columns_of_&table_name._isna;
		set columns_of_&table_name._isna;
		rownum=_n_;
	;
	run;

	%random(2);	
	proc sql noprint;
		create table &table_name._analysis
		as
		select *, case when i = 1 then 'Nulls'
				   else 'Not Nulls'
				   end as 'Values\Columns'n
		from rand
		;
		quit;
	%drop_column(&table_name._analysis, i);
	%drop_column(&table_name._analysis, u);
	%clean(rand);
	
	%DO i=1 %TO &num_of_cols;

		proc sql noprint;
			select name
			into: current_col
			from columns_of_&table_name._isna
			where rownum = &i
		;
		quit;

		%value_counts(&table_name._isna, &current_col);
		%rename_table(&table_name._isna_valuecounts, smr_&current_col);
		%replace(smr_&current_col, &current_col, 'True','Nulls');
		%replace(smr_&current_col, &current_col, 'False','Not Nulls');
		%swap_columns(smr_&current_col, &current_col, freq);

		proc sql noprint;
		create table &table_name._analysis
		as
		select 
			   a.*,
			   b.freq as &current_col.

		from &table_name._analysis as a
		left join smr_&current_col. as b
		on a.'Values\Columns'n = b.&current_col.
		;
		quit;

		%clean(smr_&current_col.);
		

	%END;
		%clean(columns_of_&table_name._isna);


%mend isna;


%macro replace_value(table_name, column_name, value_to_replace, replace_by);

	%let libname = WORK;
	%save_origin_cols_order(&table_name.);
	%type(&column_name.);
				
	%if &type = "char" %then
		%do; 
			  	proc sql;
					create table &table_name.
					as

					select *,
							 case
							 	 when &column_name. = trim(&value_to_replace.)
								 	 then "&replace_by."
							     else 
								 	 &column_name.

								 end as &column_name._new


					from &libname..&table_name.
					;
				quit;
				%drop_column(&table_name., &column_name.);
				%rename_column(&table_name., &column_name._new, &column_name.);

		%end;


	%else 
		%do;
				proc sql;
					create table &table_name.
					as

					select *,
							 case
							 	 when &column_name. = &value_to_replace.
								 	 then &replace_by.
							     else 
								 	 &column_name.

								 end as &column_name._new


					from &libname..&table_name.
				;
				quit;
				%drop_column(&table_name., &column_name.);
				%rename_column(&table_name., &column_name._new, &column_name.);
		%end;	


		    %back_origin_cols_order(&table_name.);

%mend replace_value;


%macro type(table_name, variable);

	%let libname = WORK;
	%let table_name = %upcase(&table_name);
	%let variable = %upcase(&variable);
	%global type;

	proc sql noprint;
		select lower(type)
		into: type
		from dictionary.columns
		where upcase(libname) = "&libname."
		and upcase(memname) = upcase("&table_name.")
		and upcase(name) = "&variable."
	;
	quit;


%mend type;



%macro save_origin_cols_order(table_name);

	%let libname = WORK;
	%global original_order;

	proc sql noprint;
		select upper(trim(name))
		into: original_order separated by ","
		from dictionary.columns
		where upcase(libname) = "&libname."
		and memname = upcase("&table_name.")
	;
	quit;

%mend save_origin_cols_order;


%macro back_origin_cols_order(table_name);

	%let libname = WORK;
	proc sql;
		create table &table_name.
		as
		select &original_order
		from &libname..&table_name.
		;
	quit;

%mend back_origin_cols_order;



%macro appear(table_name, col_name, value);

	%let libname = WORK;

			proc sql;
			create table temp
			as
			SELECT &col_name.,
								case 
									when &col_name. = &value.
									then 1 
									else 0
									end as ind

			FROM &libname..&table_name.
			;
			quit;

			proc sql;
			select 
				   case when SUM(ind)> 0 then "True"
				   else "False"
				   end as result
		    from temp
			;
			quit;
	
	%clean(temp);


%mend appear;
