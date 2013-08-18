# --------------------------------------------------------------------------------  
# NAME
#	Qs - Not needed unless you work on the QS project
# RESPONSIBILITIES
#
# COLLABORATIONS
#
# History
# --------------------------------------------------------------------------------  
# $log$
# --------------------------------------------------------------------------------  

package Qs;
use strict;
use Plugin;
use SessionMgr;
use Settings;
use Text::Wrap;
use Getopt::Long;
use Misc;
use Posix;
Getopt::Long::Configure("no_ignore_case");
Getopt::Long::Configure("bundling");


use vars qw 
(
 @ISA
 $COMMANDS
);

@ISA = qw (
	   Plugin
);
# ------------------------------------------------------------
sub new {
	my ($That) = @_;
	my $class = ref($That) || $That;
	my $this;

	$this->{commands} = $COMMANDS;

	bless ($this, $class);

	return $this;
}



# ------------------------------------------------------------
$COMMANDS->{qerr} =
  {
   alias => "qerr",
   description => "scan process_errors",
   synopsis => 
   [
    [],
    ["-l",undef,"no not aggregate matching messages"],
    ["-e",undef,"only list exception"],
    ["-t","hours","go back hours hours (default = 8)"],
    ["-d","date","specify date as DD.MM."],
    ["-p","pattern","restrict to procedures matching pattern"],
    ["-P","pattern","restrict to msgs matching pattern"],
    ["-V","pattern","exclude msgs matching pattern"],
    ["-T",undef,"Check for invalid targettimes only"],
    ["-I",undef,"Check for invalid data only"],
    ["-C",undef,"Check for consolidation errors"],
    ],
   routine => \&qsErrors,
   };

sub qsErrors {
	my ($this, $argv, $opts) = @_;

	my $hours = $opts->{t} ? $opts->{t} : 8;
	columnFormat->setInitially ('ERROR_MESSAGE','a40');

	my $script;
	my $timeClause;
	$timeClause .= " and trunc(timestamp) = to_date('$opts->{d}','dd.mm.')" if $opts->{d};
	if (!$timeClause) {
		$timeClause = "and timestamp > sysdate - $hours/24";
	}
	my $matchClauseMsg;
	my $matchClauseProc;
	my $excluseClauseMsg;
	if ($opts->{P}){
	 	$matchClauseMsg="and upper(error_message) like upper('%"
			.$opts->{P}
			."%')";
	}
	if ($opts->{V}){
	 	$excluseClauseMsg="and upper(error_message) not like upper('%"
			.$opts->{V}
			."%')";
	}

	if ($opts->{p}){
	 	$matchClauseProc="and upper(procedure_name) like upper('%"
			.$opts->{p}
			."%')";
	}
	my $matchClause = "\n $matchClauseMsg $matchClauseProc $excluseClauseMsg\n";
	my $type_clause .= "\nand exception_type='E'" if $opts->{e};

	my $beautifedMessage = "replace(replace (
	   error_message,
	   '). Oracle error: (ORA-0000: normal, successful completion).',
	   ''), 'Application error: (','')";
	#----------------------------------------
	if ($opts->{C}) {
		$script = qq[
			select 
				timestamp, procedure_name, $beautifedMessage
			from 
				isb.process_errors 
			where 
				upper (procedure_name) in ('SEND_CENTER')
				$timeClause $matchClause
			order by sequence_num
		];
		my $sth=sql->prepare($script);
		$sth->execute;
		my $msg;
		my $summary = {};
		while (my $row = $sth->fetchrow_arrayref) {
			$msg = $row->[2];
			$msg =~ /\@C(..)/;
			my $center = $1;

			my $status = "unknown";
			if ($msg =~ /iteration.*successful/) {
				$status = "succeeded";
			}
			if ($msg =~ /Giving up/) {
				$status = "*** falied";
			}
			if ($msg =~ /Will retry/) {
				$status = "*** oopsed";
			}
			$summary->{$center} .= "\t$status at $row->[0]\n";
		}
		# print summary:
		for (my $c=11; $c<=33; $c++) {
			if ($summary->{$c}) {
				print "transmission to $c:\n$summary->{$c}";
			} else {
				print "transmission to $c: \n\t*** not seen\n";
			}
		}
		return;
	}

	#----------------------------------------
	if ($opts->{T}) {
		$script = qq[
			select 
				max(timestamp) "missing tagettimes seen last", 
				count(*) times , 
				substr(error_message,111,22)||'...' "ctr/prod/mach/prog"
			from 
				isb.process_errors 
			where 
				error_message like '%No isb_v_target%'
				$timeClause $matchClause
			group by substr(error_message,111,22)
		];
		sql->run($script);
		return;
	}
	#----------------------------------------
	if ($opts->{I}) {
		$script = qq[
			select 
				max(timestamp) "invalid data seen last", 
				count(*) times , 
				substr(error_message,144,18) ||'...' "sol/slv/prod/machine/prog"
			from 
				isb.process_errors 
			where 
				error_message like '%no valid record%'
				$timeClause $matchClause
			group by substr(error_message,144,18)
		];
		sql->run($script);
		return;
	}
	#----------------------------------------

	unless ($opts->{l}) {
	#----------------------------------------
		$script= qq[
			select 	
				min(timestamp) first_time, 
				max(timestamp) last_time, 
			count(*) times, procedure_name, $beautifedMessage error_message
			from
				  isb.process_errors
			where
				  1=1 $timeClause $type_clause $matchClause
			group by
				  procedure_name, $beautifedMessage
			order by max(timestamp)
		];
	}else {
	#----------------------------------------
		$script=qq[
			select 
				timestamp, 
				procedure_name, 
				to_char(procedure_location) location , 
				$beautifedMessage error_message
			from
				 isb.process_errors
			where
				1=1 $timeClause $matchClause $type_clause
				order by sequence_num
			];
	}
	sql->run($script);
	return;
	#--------------------------------------------------------------------------------

}

# ------------------------------------------------------------
$COMMANDS->{qpartitions} =
  {
   alias => "qp",
   description => "return partition number",
   synopsis => 
   [
    [],
    ["-d","days", "go back n days"],
    ],
   routine => \&qpartition,
   };

sub partitionOfDate {
	my ($date) = @_;
	return "(MOD(TO_NUMBER(TO_CHAR($date, 'J')), 16) + 1)";
}

sub partitionOfDateString {
	my ($dateString) = @_;
	return partitionOfDate (qq( (to_date('$dateString','dd.mm.yyyy'))));
}

sub qpartition {
	my ($this, $argv, $opts) = @_;
	my $days = $opts->{d} ? $opts->{d} : 0;
	my $script = qq[
		select to_char(sysdate+1-$days,'dd.mm.') day,
		MOD(TO_NUMBER(TO_CHAR(sysdate+1-$days, 'J')), 16) + 1 partition
		from dual
		union
		select to_char(sysdate-$days,'dd.mm.') day,
		MOD(TO_NUMBER(TO_CHAR(sysdate-$days, 'J')), 16) + 1 parition
		from dual
		union
		select to_char(sysdate-1-$days,'dd.mm.') day,
		MOD(TO_NUMBER(TO_CHAR(sysdate-1-$days, 'J')), 16) + 1 parition
		from dual
	];

	sql->run($script);
}

# ------------------------------------------------------------
$COMMANDS->{qLoadSpeed} =
  {
   alias => "qls",
   description => "produce a histogram of load speed",
   synopsis => 
   [
    [],
    ["-r", undef,"check very recent files only"],
    ["-R", undef,"check recent files only"],
    ["-d", "days","check days days in the past instead today"],
    ["-a", undef,"print only hourly averages for today"],
    ["-A", undef,"print only averages for all days"],
    ["-H", undef,"group by hours instead of days"],
    ],
   routine => \&qloadspeed,
   };

sub qloadspeed {
	my ($this, $argv, $opts) = @_;
	my $days=0;
	my $script;

	if ($opts->{a}) {
		my $rawScript = qq[
		select 
			day, 
			"total loaded", 
			"avg loaded", 
			"avg inserted", 
			"msec/loaded",
			"msec/inserted" 
		from
		(
			select 			
				to_char(successful_last,'DD.MM.YYYY HH24') day,
				to_char(sum (successful_loaded)/1000, '990.00') || ' k' "total loaded",
				avg(successful_duration) "avg time/s",
				trunc(avg(successful_loaded)) "avg loaded",
				trunc(avg(successful_inserted_mp + successful_inserted_cm)) "avg inserted",
				1000* sum(successful_duration) 
					/ (0.1+sum(successful_loaded)) "msec/loaded",
				1000* sum(successful_duration)
					/ (0.1+ sum(successful_inserted_mp + successful_inserted_cm)) "msec/inserted"
			from theTable
				-- check yesterdays partition too
				where partition_nr between 
					MOD(TO_NUMBER(TO_CHAR(sysdate-$days, 'J')), 16) 
					and
					MOD(TO_NUMBER(TO_CHAR(sysdate-$days, 'J')), 16) + 1
			group by 
				trunc(successful_last),
				to_char(successful_last,'DD.MM.YYYY HH24')
		)
		];

		print "New interface:\n";
		$script = $rawScript;
		$script =~ s/theTable/isb_tb_loaded_files/;
		sql->run($script);

		print "\nOld interface:\n";
		$script = $rawScript;
		$script =~ s/theTable/loaded_files/;
		sql->run($script);

		return;
	}

	if ($opts->{"r"} || $opts->{"R"}) { 
		my $timeClause = "and successful_last > sysdate - 1/48";
	        $timeClause = qq(and successful_last > sysdate - 10/(24*60)) if $opts->{"r"};
		my $rawScript = qq[
			select 			
				sorting_machine_nr machine,
				successful_last rawTime,
				to_char(successful_loaded/1000, '990.00') || ' k' "loaded",
				successful_duration "avg time/s",
				successful_inserted_mp + successful_inserted_cm "inserted",
				1000* successful_duration 
					/ (0.1+successful_loaded) "msec/loaded",
				1000* successful_duration 
					/ (0.1+ successful_inserted_mp + successful_inserted_cm) "msec/inserted"
			from theTable 
				where partition_nr between 
					MOD(TO_NUMBER(TO_CHAR(sysdate-$days, 'J')), 16) 
					and
					MOD(TO_NUMBER(TO_CHAR(sysdate-$days, 'J')), 16) + 1
				$timeClause
			order by rawTime
		];

		print "New interface:\n";
		$script = $rawScript;
		$script =~ s/theTable/isb.isb_tb_loaded_files/;
		sql->run($script);

		print "\nOld interface:\n";
		$script = $rawScript;
		$script =~ s/theTable/isb.loaded_files/;
		sql->run($script);

		return;
	}

	if ($opts->{A}) {
		my $rawScript = qq{
		select 
			day, 
			"total loaded", 
			"avg loaded", 
			"avg inserted", 
			"msec/loaded",
			"msec/inserted" 
		from
		(
			select 			
				to_char(successful_last,'DD.MM.YYYY') day,
				to_char(sum (successful_loaded)/1000000, '0.00') || ' Mio' "total loaded",
				avg(successful_duration) "avg time/s",
				trunc(avg(successful_loaded)) "avg loaded",
				trunc(avg(successful_inserted_mp + successful_inserted_cm)) "avg inserted",
				1000* sum(successful_duration) 
					/ (0.1+sum(successful_loaded)) "msec/loaded",
				1000* sum(successful_duration)
					/ (0.1+ sum(successful_inserted_mp + successful_inserted_cm)) "msec/inserted"
			from theTable
			group by 
				trunc(successful_last),
				to_char(successful_last,'DD.MM.YYYY')
		)
		};

 		print "New interface:\n";
		$script = $rawScript;
		$script =~ s/theTable/isb.isb_tb_loaded_files/;
		sql->run($script);

		print "\nOld interface:\n";
		$script = $rawScript;
		$script =~ s/theTable/isb.loaded_files/;
		sql->run($script);

		return;
	}


	$days = $opts->{d} if $opts->{d};
	$script = qq (
		select 
			successful_duration "load_time(sec)", 
			count(*) ocurrences 
		from isb.isb_tb_loaded_files 
		where partition_nr = MOD(TO_NUMBER(TO_CHAR(sysdate-$days, 'J')), 16) + 1
		group by successful_duration
	);
	sql->run($script);
	$script = qq (
		select to_char(sysdate - $days,'DD.MM.YYYY') day, 
		1000 * sum(successful_duration)/(0.1 + sum(successful_loaded)) "msec/rec"
		from isb.isb_tb_loaded_files 
		where partition_nr = MOD(TO_NUMBER(TO_CHAR(sysdate-$days, 'J')), 16) + 1
	);
	sql->run($script);

	my $script = qq (
		select 
			successful_duration "load_time(sec)", 
			count(*) ocurrences 
		from isb.loaded_files 
		where partition_nr = MOD(TO_NUMBER(TO_CHAR(sysdate-$days, 'J')), 16) + 1
		group by successful_duration
	);
	sql->run($script);
	$script = qq {
		select to_char(sysdate - $days,'DD.MM.YYYY') day, 
		avg(successful_duration) "average old ifc (sec)"
		from isb.loaded_files 
		where partition_nr = MOD(TO_NUMBER(TO_CHAR(sysdate-$days, 'J')), 16) + 1
	};
	sql->run($script);

}

# ------------------------------------------------------------
$COMMANDS->{qInsertRatio} =
  {
   alias => "qir",
   description => "show insert ratio",
   synopsis => 
   [
    [],
    ["-d", "day", "the day to check"],
    ["-H", "hours", "go back hours"],
    ["-I", undef, "use insert_date not process_date"],
    ["-F", undef, "count files not mailpieces"],
    ["-S", undef, "show sortplan statistics"],
    ["-7", undef, "austria specfic: exclude 700 plans"],
    ],
   routine => \&qInsertRatio
};

sub qInsertRatio {
	my ($this, $argv, $opts) = @_;

	my $day;
	if ($opts->{d}) {
		$day = "to_date('$opts->{d}','DD.MM.')";
	} else {
		$day = "trunc(sysdate)";
	}

	my $timeField = $opts->{I} ? "successful_last" : "process_date_and_time";
	my $timeClause;
	if ($opts->{H}) {
		$timeClause = "$timeField > sysdate - $opts->{H}/24";
	} else {
		$timeClause = "trunc($timeField) = $day";
	}

	my $sevenHundredClause;
	if ($opts->{7}) {
		$sevenHundredClause = qq(and sol_id not in (
		select sol_id from isbsol
		where sol_no between 700 and 799
		));
	}

	# ----------------------------------------
	my $mpcsScript = qq(
	select
	sm.sorting_machine_nr machine, sm.type, lf.day, lf.insertratio, lf.loaded, lf.lost, files
	from
	(
		select 
			trunc(process_date_and_time) day,
			sorting_machine_nr,
			trunc(100 * sum(successful_inserted_mp + successful_inserted_cm)/
			sum(successful_loaded +1),2)
			InsertRatio,
			sum(successful_loaded) loaded,
			sum(successful_loaded -successful_inserted_mp  -successful_inserted_cm)
			lost,
			count(*) files
		from isb_tb_loaded_files
		where $timeClause $sevenHundredClause
		group by
			sorting_machine_nr,
			trunc(process_date_and_time)
	) lf,
	sorting_machines sm,
	center_header ch
	where sm.center_nr = ch.center_nr
	and sm.sorting_machine_nr = lf.sorting_machine_nr
	);

	# ----------------------------------------
	my $filesScript = qq (
	select 		
		sorting_machine_nr machine,
		sol.sol_name,	
		sol.sol_id,
		-- successful_last rawTime,
		decode(
			sign(successful_loaded) 
			- sign (successful_inserted_mp + successful_inserted_cm),
			1, 'all lost',
			0, 'some loaded',
			'strange'
		) state,
		count(*),
		min(process_date_and_time) ealiest,
		max(process_date_and_time) latest
	from 
		isb.isb_tb_loaded_files lf,
		ISB_TB_SORTING_LIST sol
	where   $timeClause
		and sol.sol_id = lf.sol_id
		-- ignore almost empty files
		and successful_loaded >20
	group by 
		sorting_machine_nr,
		sol.sol_name,	
		sol.sol_id,
		decode(
			sign(successful_loaded) 
			- sign (successful_inserted_mp + successful_inserted_cm),
			1, 'all lost',
			0, 'some loaded',
			'strange'
		)
	);
	# ----------------------------------------
	my $sortplanScript = qq (
	select 		
		sol.sol_name,	
		sol.sol_id,
		sorting_machine_nr machine,
		-- successful_last rawTime,
		decode(
			sign(successful_loaded) 
			- sign (successful_inserted_mp + successful_inserted_cm),
			1, 'all lost',
			0, 'some loaded',
			'strange'
		) state,
		count(*),
		min(process_date_and_time) ealiest,
		max(process_date_and_time) latest
	from 
		isb.isb_tb_loaded_files lf,
		ISB_TB_SORTING_LIST sol
	where 1=1
		and $timeClause
		and sol.sol_id = lf.sol_id
		and successful_loaded >20
	group by 
		sorting_machine_nr,
		sol.sol_name,	
		sol.sol_id,
		decode(
			sign(successful_loaded) 
			- sign (successful_inserted_mp + successful_inserted_cm),
			1, 'all lost',
			0, 'some loaded',
			'strange'
		)
	);
	my $script = $opts->{F} ? $filesScript : $mpcsScript;
	my $script = $opts->{S} ? $sortplanScript : $script;

	print "new ifc\n";
	sql->run($script);
	if ($opts->{F} or $opts->{7}) {
		print "no such statistics for old ifc for now\n"
	} else {
		print "old ifc\n";
		$script =~ s/isb_tb_loaded_files/loaded_files/;
		sql->run($script) unless $opts->{S};
	}
}


# ------------------------------------------------------------
$COMMANDS->{loadedFiles} =
  {
   description => "select relevant columns from loadedFiled",
   synopsis => 
   [
    [],
    ],
   routine => \&loadedFiles
};
sub loadedFiles {
	my ($this, $argv, $opts) = @_;
	my $clause = "@$argv";

	sql->run (qq( select
		SOL_ID,
		SLV_NUMBER,
		CODING_MACHINE_NR codmach,
		MAILPIECE_NR mpc_nr,
		MONTH_DAY_INTERVAL mdi,
		MONTH_NR m,
		RECOGNITION rec,
		MAIL_CLASS class,
		SORTCODE,
		PROCESS_DATE_AND_TIME,
		STACKER_NR,
		PRODUCT_ID prod,
		SORTING_MACHINE_NR sorma,
		SUCCESSFUL_LOADED loaded,
		SUCCESSFUL_INSERTED_MP ins_mp,
		SUCCESSFUL_INSERTED_CM ins_cm,
		trunc(100* (SUCCESSFUL_INSERTED_MP + SUCCESSFUL_INSERTED_CM)/(SUCCESSFUL_LOADED + 0.1))||'%' ratio,
		SUCCESSFUL_LAST
	from isb_tb_loaded_files
	$clause
	));
}
# ------------------------------------------------------------
$COMMANDS->{qMailpieces} =
  {
   alias => "qmp",
   description => "count mailpieces",
   synopsis => 
   [
    [],
    ["-d", "day", "the day to check"],
    ["-m", "machine_nr", "the sorting machine"],
    ["-c", undef, "print codedSortedInd"],
    ],
   routine => \&qMailpieces
};

sub qMailpieces {
	my ($this, $argv, $opts) = @_;

	my $day;
	if ($opts->{d}) {
		$day = "to_date('$opts->{d}','DD.MM.')";
	} else {
		$day = "trunc(sysdate)";
	}
		my $codedSorted = "coded_sorted_ind,"if ($opts->{d});
		my $codedSortedMp = "mpcs.coded_sorted_ind mp_cs,"if ($opts->{d});
		my $codedSortedCm = "cmp.coded_sorted_ind cmp_cs,"if ($opts->{d});
	my $machineClause = "and sorting_machine_nr = $opts->{m}" if $opts->{"m"};
	my $script = qq(
	select distinct
		mpcs.sorting_machine_nr,
		mpcs.day,
		mpcs.n mpcs,
		cmp.n cmpcs,
		mpcs.sorted mpcs_sorted,
		mpcs.coded,
		mpcs.endsorted,
		mpcs.endcoded,
		cmp.sorted cm_sorted,
		cmp.coded,
		cmp.endsorted,
		cmp.endcoded,
		(mpcs.n  + nvl(cmp.n,0) ) total
	from
		(
		select 
			sorting_machine_nr, 
			trunc(process_date_and_time) day,
			sum(decode(coded_sorted_ind,0,1,0)) sorted,
			sum(decode(coded_sorted_ind,1,1,0)) coded,
			sum(decode(coded_sorted_ind,2,1,0)) endsorted,
			sum(decode(coded_sorted_ind,3,1,0)) endcoded,
			count(*) n
		from mailpieces
		where partition_nr = 
			MOD(
				TO_NUMBER(
					TO_CHAR(
						to_date('$opts->{d}','DD.MM.'),
						'J'
				      	)
				),
				16
			) + 1
		and trunc(process_date_and_time) = $day
		$machineClause
		group by
			trunc(process_date_and_time),
			$codedSorted
			sorting_machine_nr
		) mpcs,
		(
		select 
			sorting_machine_nr, 
			trunc(process_date_and_time) day,
			sum(decode(coded_sorted_ind,0,1,0)) sorted,
			sum(decode(coded_sorted_ind,1,1,0)) coded,
			sum(decode(coded_sorted_ind,2,1,0)) endsorted,
			sum(decode(coded_sorted_ind,3,1,0)) endcoded,
			count(*) n
		from consolidation_mailpieces
		where trunc(process_date_and_time) = $day
		$machineClause
		group by
			trunc(process_date_and_time),
			$codedSorted
			sorting_machine_nr
		) cmp
	where mpcs.sorting_machine_nr = cmp.sorting_machine_nr(+)
	and mpcs.day = cmp.day(+) 
	);

	sql->run($script);
}

# ------------------------------------------------------------
$COMMANDS->{qTargetTimes} =
  {
   alias => "qtt",
   description => "return valid sorting lists and targettimes",
   synopsis => 
   [
    [],
    ["-C", "centerNr", "restrict to given center (28)"],
    ["-P", "program", "sort program number"],
    ["-M", "machine", "soring machine nr"],
    ],
   man => qq (
),
   routine => \&qslv,
   };

sub qslv {
	my ($this, $argv, $opts) = @_;


	my $machineProgramClause;
	if ($opts->{P}) {
		$machineProgramClause .= "	AND    s.sort_program_nr = $opts->{P}";
	}
	if ($opts->{M}) {
		$machineProgramClause .= "	AND    tt.sorting_machine_nr = $opts->{M}";
	}
	if ($opts->{C}) {
		$machineProgramClause .= "		AND  tt.center_nr = $opts->{C}";
	}

	my $script = qq[
	SELECT
		m.type machine,
		tt.sorting_machine_nr nr,
		p.product_id,
		p.quality_target_1 qt1,
		p.quality_target_2 qt2, -- bha 070999
	       	TO_DATE(TO_CHAR(sysdate,'ddmmyyyy') || TO_CHAR(tt.sort_program_start_time,'hh24mi'),'ddmmyyyyhh24mi')
			start_date,
	       	TO_DATE(TO_CHAR(sysdate,'ddmmyyyy') || TO_CHAR(tt.sort_program_end_time,'hh24mi'),'ddmmyyyyhh24mi')
			end_date,
	       	tt.sort_program_seq_num,
		s.sort_program_nr
	FROM
		sorting_machines m,
		isb_v_target_times tt,
		products p,
		isb_v_sort_programs s
	WHERE  1=1
	AND    tt.center_nr = s.center_nr
	AND    s.sequence_num = tt.sort_program_seq_num
	$machineProgramClause
	AND    tt.sort_program_end_time IS NOT NULL
	AND    tt.product_id = p.product_id
	AND    tt.sorting_machine_nr = m.sorting_machine_nr
	AND    tt.validity_start_date =
	(
	       SELECT MAX(tt1.validity_start_date)
	       FROM   isb_v_target_times tt1
	       WHERE  tt1.center_nr = tt.center_nr
	       AND    tt1.product_id = p.product_id
	       AND    tt1.sort_program_seq_num = s.sequence_num
	       AND    tt1.sorting_machine_nr = tt.sorting_machine_nr
	       AND    tt1.sort_program_end_time IS NOT NULL
	       AND    TRUNC(tt1.validity_start_date) <= TRUNC(sysdate)
	 )
	ORDER BY nr, product_id
	];

	sql->run($script);

}

# ------------------------------------------------------------
$COMMANDS->{getCuttofDate} =
  {
   alias => "gcd",
   description => "return the time after which a mpcs is considered delayed",
   synopsis => 
   [
    [],
    ["-I", "id", "mailpieceId"],
    ["-P", "program", "sort program nr"],
    ["-T", "hours", "Quality target"],
    ["-c", "class", "Product ID"],
    ["-C", "center", "center Nr"],
    ["-v", undef, "be verbose"],
    ],
   routine => \&gcd,
   };

sub gcd_time {
	my ($mdi, $add) = @_;
	my $minutesStart = int($mdi%20 * 24*60/20) + $add*24*60/20;
	my $hour = int($minutesStart/60);
	my $minutes = $minutesStart%60;
	return sprintf("%02d:%02d", $hour,$minutes);
}

sub gcd {
	my ($this, $argv, $opts) = @_;
	if (!($opts->{I} and $opts->{P})) {
		print "You must specify -I and -P \n";
		return;
	}
	# get the center number
	my $centerNr = $opts->{C};
	if (!$centerNr) {
		$centerNr = sql->runArrayref("select center_nr from center_header")->[0]->[0];
	}

	# Quality target defaults to 0
	my $qt = $opts->{T} ? $opts->{T} : 0;

	# ProductId defaults to 'A'
	my $mailClass = $opts->{c} ? $opts->{c} : 'A';

	# get the sort program sequence num
	my $spSeqNum = sql->runArrayref(qq(
		select sequence_num 
		from isb_tb_sort_programs
		where center_nr = $centerNr
		and sort_program_nr=$opts->{P}
	))->[0]->[0];

print substr($opts->{I},0,-9)."\n";
	my $monthDayInterval=substr(substr($opts->{I},0,-7),-3,3);
	my $monthNo = substr($opts->{I},0,-10) + 0;
	# compute the coding time
	if ($opts->{v}) {
		print "Mailpiece    = $opts->{I}\n";
		print "CenterNr     = $centerNr\n";
		print "QualtyTarget = $qt\n";
		print "MailClass    = $mailClass\n";
		print "MonthNo      = $monthNo\n";
		print "MonthDay     = $monthDayInterval\n";
		print "CodingDay    = ". (1 + int($monthDayInterval/20)) ."\n";
		print "CodingTime   = " . gcd_time($monthDayInterval,-1).'-'.
		  gcd_time($monthDayInterval,0)."\n";
		print "Program      = $opts->{P}\n";
		print "SpSeqnum     = $spSeqNum\n";
	}
	if (!$spSeqNum) {
		print "Sort program $opts->{P} not known in center $centerNr\n";
		return;
	}
	sql->run("begin insert_mailpieces_package.setCenterParameters ($centerNr); end;");
	sql->run(qq(
		select (to_char(
			insert_mailpieces_package.endOfCodingDay ($monthDayInterval,$monthNo),
			'DD.MM.YY HH24:MI'
		)) "end of doding day" from dual));
	sql->run(qq(
		select to_char(insert_mailpieces_package.getCutoffDate (
			insert_mailpieces_package.endOfCodingDay ($monthDayInterval,$monthNo),
			$qt,
			'$mailClass',
			$spSeqNum
		),'DD.MM.YY HH24:MI') cutoff from dual
	));

}



1;


# ------------------------------------------------------------
$COMMANDS->{findLoadedFile} =
  {
   alias => "flf",
   description => "Find a loaded file row from mailpiece id",
   synopsis => 
   [
    ["mailpieceId"],
    ["-c",undef,"use consolidation mpcs instead of mpcs"],
    ["-v",undef,"show more columns"],
    ],
   routine => \&findLoadedFile,
   };

sub findLoadedFile {
	my ($this, $argv, $opts) = @_;
	my $mpid = $argv->[0] or return;

	my $verboseColumns = qq(
			,
			successful_Rejected,
			successful_Inserted_Mpc,
			successful_Inserted_Mps,
			successful_Inserted_Bbz,
			successful_Duration,
			successful_Duration_Mp,
			successful_Duration_Cm,
			successful_Duration_Mpc,
			successful_Duration_Mps,
			successful_Duration_Bbz,
			successful_Duration_Chk,
			successful_Duration_Del,
			successful_Count,
			successful_Last,
			not_Completed_Count,
			not_Completed_Last,
			duplicated_Count,
			duplicated_Last
	) if $opts->{v};
	my $mailpieces = sql->runArrayref(qq(
		select 
			mailpiece_id, 
			partition_nr, 
			sorting_machine_nr, 
			to_char(insert_date_and_time,'dd.mm. hh24:mi'),
			to_char(process_date_and_time,'dd.mm. hh24:mi:ss')
		from mailpieces
		where mailpiece_id=$mpid
	));


	foreach my $mp (@$mailpieces) {
		printf ("--------------------------------------------------\n");
		printf ("%-12s|%-10s|%-10s|%-12s|%-12s\n", "id", "part", "sorma", "inserted", "processed");
		printf ("%12s|%10s|%10s|%12s|%12s\n",
			$mp->[0], $mp->[1], $mp->[2], $mp->[3], $mp->[4]
			);

		sql->run(qq(
		select 
			Partition_Nr,
			sol_Id,
			slv_Number,
			coding_Machine_Nr,
			mailpiece_Nr,
			month_Day_Interval ,
		      	month_Nr,
			recognition,
			recognition_State rec_state,
			recognition_Unit rec_unit,
			mail_Class,
			sortcode      ,
			id_Print,
			process_Date_And_Time,
			sorter_No,
			stacker_Nr,
			special_Sort sso,
			run_Start_Time,
			run_No,
			sort_Status,
			carrier_Id,
			product_Id,
			sorting_Machine_Nr,
			mca_Code_Format fomat,
			file_Name    ,
			insert_Date_And_Time,
			successful_Loaded,
			successful_Inserted_Mp
			successful_Inserted_Cm
			$verboseColumns
		from isb_tb_loaded_files lf
		where  1=1
		and	lf.partition_nr = $mp->[1]
		and	lf.sorting_machine_nr = $mp->[2]
		and	trunc(lf.successful_last,'mi') = to_date('$mp->[3]','dd.mm. hh24:mi')
	));
	}
}

# ------------------------------------------------------------
$COMMANDS->{mlogs} =
  {
   alias => "",
   description => "list snaptime and count of mlogs",
   synopsis => 
   [
    ["pattern"],
    ],
   routine => \&mlogs,
   };

sub mlogs {
	my ($this, $argv, $opts) = @_;
	my $nameClause;
	if ($argv->[0]) {
		$nameClause = "and object_name like '$argv->[0]'";
	};


	my $sth_obj = sql->prepare (qq(
		select object_name
		from user_objects
		where object_type ='TABLE'
		AND object_name like 'MLOG\$%'
		$nameClause
	));
	$sth_obj->execute;
	printf ("%-32s |%16s |%9s\n", 'Mlog','min Snaptime','count');
	for (my $i=0; $i<(32+16+9 + 4); $i++) {
		print "-";
	}
	print "\n";
	while (my $row=$sth_obj->fetchrow_arrayref) {
		my $sth=sql->prepare(qq(
			select '$row->[0]', min (snaptime\$\$), count(*) from $row->[0]
		));
		$sth->execute;
		while (my $row=$sth->fetchrow_arrayref) {
			printf ("%-32s |%16s |%9s\n", initcap($row->[0]), $row->[1], $row->[2]);
		}
	}

}

# ------------------------------------------------------------
$COMMANDS->{sortingMachines} =
  {
   alias => "sorma",
   description => "show a list of sortingMachines",
   synopsis => 
   [
    [],
    ["-c","centerNr","do not use own center"], 
    ["-p","pattern","machine type"], 
    ],
   routine => \&sortingMachines,
   };

sub sortingMachines {
	my ($this, $argv, $opts) = @_;
	my $centerClause;
	if ($opts->{c}) {
		$centerClause = "and center_nr = ".$opts->{c}
	} else {
		$centerClause = "and center_nr = (select center_nr from center_header)"
	}
	my $patternClause = "and upper(type) like ".
	  "'%".uc($opts->{p})."%'" if $opts->{p};
	sql->run(qq(
		select 
			center_nr,
			sorting_machine_nr nr,
			type,
			remote_path,
			tcpip_address ip,
			sorting_machine_active active
		from sorting_machines
		where 1=1
		$centerClause
		$patternClause
	));
}

# ------------------------------------------------------------
$COMMANDS->{qSortplans} =
  {
   alias => "qsp",
   description => "Display missing SPM interace information",
   synopsis => 
   [
    ["remainingArg"],
    ["-a",undef,"show all plans not just the missing ones"],
    ["-s","sortplan name","restrict to this sortplan"],
    ["-V",undef,"compare version numbers"],
    ["-M",undef,"show machines too"],
    ["-D","databaseLink","use this link instead of CC01"],
    ],
   routine => \&qSortplans,
   };

sub qSortplans {
	my ($this, $argv, $opts) = @_;
	my $dbLink = $opts->{D} ? $opts->{D} : "CC01.WORLD";
	my $versionClause = "and i.slv_number = slv.slv_number" if $opts->{V};
	my $extraColumns = q(
			,prs.prs_name
			,prs.prs_number
			,prs.prs_id
			,slv_creation
			,slv_generation
			slv_download  
	) if $opts->{M};

	my $sortplanClause = qq(and sol_name = $opts->{"s"}) if $opts->{"s"};
	my $missingClause = qq(
		and not exists (
			select 1
			from isbslv i
			where i.dve_id = slv.dve_id
			and i.sol_id = slv.sol_id
			$versionClause
       		)
) unless $opts->{a};

	sql->run (qq(
		select distinct
			sol.dve_id, 
			sol.sol_id,
		      	sol.sol_name,
			slv.slv_number
			$extraColumns
		from 
			spm_tb_sorting_list\@$dbLink sol,
			spm_tb_sorting_list_version\@$dbLink slv,
			spm_tb_is_valid_for_resource\@$dbLink vfr,
			spm_tb_prod_resource\@$dbLink prs
		where
			slv.dve_id = sol.dve_id
		and	slv.sol_id = sol.sol_id
		and	sol.dve_id = vfr.dve_id
		and	sol.sol_id = vfr.sol_id
		and	vfr.prs_id = prs.prs_id
		$sortplanClause
		$missingClause
	));
}

# ------------------------------------------------------------
$COMMANDS->{site_check} =
  {
   alias => "",
   description => "run QS site check",
   synopsis => 
   [
    [],
    ["-m",undef,"report machines not sending data"],
    ["-i",undef,"report insert ratio"],
    ],
   routine => \&site_check,
   };

sub site_check {
	my ($this, $argv, $opts) = @_;

	if ($opts->{"m"}) {
	sql->run(qq(
		select
			sm.sorting_machine_nr no_data_from,
			sm.type sorting_machine
		from
			isb.sorting_machines sm,
			isb.center_header ch
		where not exists (
			-- old ifc:
			select 1
			from isb.loaded_files mp
			where mp.sorting_machine_nr = sm.sorting_machine_nr and
			mp.process_date_and_time > sysdate - 2
		) and not exists (
			-- new ifc:
			select 1
			from isb.isb_tb_loaded_files mp
			where mp.sorting_machine_nr = sm.sorting_machine_nr and
			mp.process_date_and_time > sysdate - 2
		) and
		sm.center_nr = ch.center_nr and
		sm.sorting_machine_active = 1
	));
	return;
	}
	if ($opts->{i}) {
		sql->run(insertRatioStatement("new"));
		sql->run(insertRatioStatement("old"));
	}
}

sub insertRatioStatement {
	my ($ifc) = @_;
	
	if ($ifc eq "new") {
	return (qq(
		select  
			sorting_machine_nr "machine", 
			sort_program_nr "program nr",
			sort_program "program",
			product_id "prod", 
			trunc(100 * (success/total)) || '%' "i-ratio (new)",
			total - success "mpc lost"
		from
		(
			select 
				sorting_machine_nr, 
				product_id, 
				sort_program_nr,
				sort_program,
				sum(successful_inserted_mp) + sum(successful_inserted_cm) success,
				sum(successful_loaded) total
			from
			(
				select 
					sorting_machine_nr, 
					decode(mail_class,'A','A','B','B2','C','B1','D','D','?') product_id, 
					sol.sol_no sort_program_nr,
					sol.sol_name sort_program,
					lf.successful_inserted_mp,
					lf.successful_inserted_cm,
					lf.successful_loaded
				from 
					isb.isb_tb_loaded_files lf,
					isb.isb_tb_sorting_list sol
				where 
					lf.process_date_and_time > trunc(sysdate)-1 and
					lf.sol_id = sol.sol_id and
					nvl(lf.slv_number,-1) > 0 
				union all
				select 
					sorting_machine_nr, 
					decode(mail_class,'A','A','B','B2','C','B1','?') product_id, 
					lf.sol_id sort_program_nr,
					'???????' sort_program,
					lf.successful_inserted_mp,
					lf.successful_inserted_cm,
					lf.successful_loaded
				from 
					isb.isb_tb_loaded_files lf
				where 
					lf.process_date_and_time > trunc(sysdate)-1 and
					nvl(lf.slv_number,-1) <= 0 
			)
			group by
				sorting_machine_nr, 
				product_id, 
				sort_program_nr,
				sort_program
		) 
		where 
			total-success > 5000
		order by 
			"machine",
			"program nr",
			"program",
			"prod"
		));
	}
	if ($ifc eq "old") {
	return (qq(
		select 	
			sorting_machine_nr machine, 
			product_id prod, 
			sort_program_nr program,
			trunc(100 * (success/total)) || '%' "i-ratio (old)",
			total - success "mpc lost"
		from
		(
			select 
				sorting_machine_nr, 
				product_id, 
				sort_program_nr,
				sum(successful_inserted_mp) + sum(successful_inserted_cm) success,
				sum(successful_loaded) total
			from 
				isb.loaded_files 
			where 
				process_date_and_time > trunc(sysdate)-1 and
				('&flags' like '%-i%'	or '&flags' like '%-A%')
			group by
				sorting_machine_nr, product_id, sort_program_nr
		) 
		where 
			total-success > 5000
		order by "i-ratio (old)"
	))
	}
}

# ------------------------------------------------------------
$COMMANDS->{codingTime} =
  {
   alias => "",
   description => "decode a mailpiece_id or day interval",
   synopsis => 
   [
    ["id"],
    ],
   routine => \&codingTime,
   };

sub codingTime {
	my ($this, $argv, $opts) = @_;

	my $id = $argv->[0];
	my $month;
	my $interval;
	my $codingMachine;
	my $mpcNo;

	if (length($id)>3) {
		$id = "000000" . $id;
		$id =~ /0*(.)(...)(..)(.....)$/;
		$month = $1;
		$interval = $2;
		$codingMachine = $3;
		$mpcNo = $4;
	}else {
		$interval=$id;
	}
	print "month            = $month\n";
	print "interval         = $interval\n";
	$interval=1 if $interval==0;
	print "codingMachine    = $codingMachine\n";
	print "mpcNo            = $mpcNo\n";
	print "day              = ". POSIX::ceil($interval/20)."\n";
	my $dayInterval = (($interval + 19) % 20) + 1;
	print "dayInterval      = $dayInterval\n";
        my $minutes2 = $dayInterval * 24 * 60 / 20;
	my $hour2 = POSIX::floor($minutes2/60);
	my $min2  = $minutes2%60;

        my $minutes1 = ($dayInterval-1) * 24 * 60 / 20;
	my $hour1 = POSIX::floor($minutes1/60);
	my $min1  = $minutes1%60;

	printf ("codingTime       = %02d:%02d - %02d:%02d\n", $hour1,$min1, $hour2,$min2);


}

# ------------------------------------------------------------
$COMMANDS->{localTotals} =
  {
   alias => "tot",
   description => "show totals for the last couple of days",
   synopsis => 
   [
    [],
    ["-d","days" ,"go back n days"],
    ["-a",undef,"show all totals, not just locat totals"],

    ],
   routine => \&localTotals,
   };

sub localTotals {
	my ($this, $argv, $opts) = @_;

	my $id = $argv->[0];
	my $days = $opts->{d} ? $opts->{d} : 1;
	my $centerClause = "and ch.center_nr = mt.center_nr" unless $opts->{a};

	sql->run(qq(
	select 
		process_date, 
		mt.center_nr,
		sum(total_auto_coded) total_auto_coded,
		sum(total_video_coded) total_video_coded,
		sum (total_processed_from) total_processed_from,
		sum (total_processed_to) total_processed_to	
	from 
		mailpiece_totals mt,
		center_header ch
	where 
		process_date > sysdate - $days
		$centerClause
	group by 
		process_date, 
		mt.center_nr
	));

}

# ------------------------------------------------------------
$COMMANDS->{machinePasses} =
  {
   description => "Produce a histogram of machine passes",
   synopsis => 
   [
    [],
    ["-d","day" ,"day in format dd.mm.yyyy"],
    ["-a",undef,"take remote mailpieces into account"],
    ["-s", "N", "analyze every Nth mailpiece (default=100)"],
    ["-c", "class", "Mailclass (A, B1). Default is A"],
    ],
   man => q(
	Produce an answer like "n mailpieces went through m passes". Either
	count only local mailpieces (from this center) or optionally take
	mailpieces also seen in other centers into account if they were coded
	in this center.

	To get data based on another coding center, connect to its QS
	QS-server.

	Data is based on sample mailpieces like every 100th mailpiece.
	You can change this with th -s option. Lower sample rates give
	more accurate results, but consume more time.
),
   routine => \&machinePasses,
   };

# The meaning of sent_ind
# Sent_Ind|count(*)|
# ------------------
#        0|1770355 | GC_SI_Not_Sent   Mailpieces coded
#        1|952163  | GC_SI_Sent       mailpieces sorted (distinguishes FROM/TO)
#        2|27254   | GC_SI_Received   mailpieces received from other centers

# GC_CSI_Sorted           = 0;
# GC_CSI_Coded            = 1;
# GC_CSI_End_Sorted       = 2;
# GC_CSI_Coded_End_Sorted = 3;

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub getDbLinks {
	# get the db links pointing to other QS servers

	my $sth=sql->prepare ("select db_link from all_db_links");
	$sth->execute;
	my @links;
	while (my $row = $sth->fetchrow_arrayref) {
		my $link=$row->[0];
		if ($link =~ /^[AC][1-3][0-9].*/) {
			push (@links, $link);
		}
	}
	return @links;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub julianDate {
	my ($day) = @_;
	my $r = sql->runArrayref(" select TO_CHAR($day, 'J') from dual");
	return $r->[0][0];
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub getPartitions {
	my ($julianDay, $tolerance) = @_;
	my $partitions="";
	for (my $i=0; $i<=$tolerance; $i++) {
		$partitions .= "," unless $i==0;
		$partitions .= (($julianDay+$i)%16) + 1;
	}
	return $partitions;
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
sub machinePasses {
	my ($this, $argv, $opts) = @_;

	# get the day right
	my $day=qq(to_date('$opts->{d}','DD,MM,YYYY'));

	unless ($opts->{d}=~/[0-3][0-9]\.[0-1][0-9].20[0-9][0-9]/) {
		print "illegal date " . $opts->{d} . "\n";
		return;

	}

	# get the sample rate right
	my $sample = 100;
	if ($opts->{"s"}) {
		$sample = $opts->{"s"};
		if ($sample <=0) {
			print "Illegal sample rate '$sample'.\n";
			return;
		}

	}

	# get the mail class right
	my $class = "'A'";
	if (my $c=$opts->{c}) {
		if ($c =~/A|B1|B2/) {
			$class = qq('$c');
		} else {
			print "illegal mail class '$c'.\n";
			return;
		}
	}

	my $scope = $opts->{a} ? "'Inw+Outw'" : "'Local'";
	sql->run(qq(select $day day, $class class, $scope scope, $sample sample_rate  from dual));

	my $remoteClause;
	if ($opts->{a}) {
		my @links = getDbLinks();
		print "Also looking in @links \n";
		foreach my $link (@links) {
			$remoteClause .= qq(
			      union all
				select /*+ PARALLEL(rem,4) */
				mailpiece_id,
				0 firstDayCoded,
				0 local,
				1 remote
				from consolidation_mailpieces\@$link rem
				where 1=1
				and sent_ind!=2
				and trunc(process_date_and_time) between  
					$day
					and 
					$day + 4
				and mod(mailpiece_id,$sample)=0
				and product_id = $class
		);
		}
	}

	my $jd = julianDate($day);
	my $partitions=getPartitions($jd,3);
	my $firstPartition=getPartitions($jd,0);

	sql->run (qq(
		select passes, local, remote, $sample*count(*) mpcs
		from
		(
			select 
				mailpiece_id, 
				sum(firstDayCoded) firstDayCoded,
				sum(local) local , 
				sum(remote) remote, 
				sum(local) + sum(remote) passes
			from (
				select /*+ PARALLEL(loc,4) */
				mailpiece_id,
				decode(partition_nr,
					$firstPartition,
					decode (coded_sorted_ind,
						1,1,
						3,1,
						0
					),
					0
				) firstDayCoded,
				1 local,
				0 remote
				from  mailpieces loc
				where 1=1
				and partition_nr in ($partitions)
				and sent_ind!=2
				and mod(mailpiece_id,$sample)=0
				--and mailpiece_id=14151901278
				and product_id = $class
			$remoteClause
			)
			group by mailpiece_id, mod(mailpiece_id,$sample)
			having sum(firstDayCoded) > 0
		)
		group by passes, local, remote
		order by passes, local, remote
	));

}


$COMMANDS->{refreshSequence} =
  {
   description => "refresh a sequnce from a maxvalue",
   synopsis => 
   [
    [],
    ["-s","sequence" ,"the sequence to refresh"],
    ["-t","table" ,"the table where to lookup the maxvalue"],
    ["-c","column","the column to lookup"],
    ["-b", "buildRule", "one of 'centerPrefix' or none"],
    ["-n", undef, "don't execute"],
    ],
   routine => \&refreshSequence,
   };

sub objectExists {
	my ($otype, $oname) = @_;
	my $ar = sql->runArrayref (qq(
		select count(*) 
		from user_objects 
		where object_name = upper('$oname')
		and object_type = upper('$otype')
	));
	return $ar->[0]->[0];
}
sub refreshSequence {
	my ($this, $argv, $opts) = @_;

	my $centerNr;
	my $ar = sql->runArrayref ( "select center_nr from center_header");
	$centerNr = $ar->[0]->[0];

	my $sequence = $opts->{"s"};
	my $table = $opts->{t};
	my $column = $opts->{c};
	my $buildRule = $opts->{b};

	# test if the sequence and the table exist
	if (! objectExists("sequence", $sequence)) {
		print "Sequence $sequence does not exist\n";
		return;
	}
	if (! objectExists("table", $table)) {
		print "Table $table does not exist\n";
		return;
	}

	# get the new startvalue
	my $startvalue;
	if ($buildRule =~ "centerPrefix") {
		my $ar = sql->runArrayref(qq(
			select max($column) - 10000000000 * $centerNr
			from $table
			where $column like '$centerNr%'
		));
		$startvalue = $ar->[0]->[0];

	} else {
		my $ar = sql->runArrayref(qq(
			select max($column) 
			from $table
		));
		$startvalue = $ar->[0]->[0];
	}
	$ar = sql->runArrayref (qq(
			select $sequence.nextval from dual));
	my $nextval = $ar->[0]->[0];

	printf ("-- %-32s %-24s %-24s %s\n",
		$sequence,
		"startvalue = $startvalue",
		"nextval = $nextval",
		($startvalue < $nextval ? " - ok" : "needs refresh")
		);

	if ($startvalue >= $nextval) {

		my $startWith = $startvalue+1;
		my $script1 = "drop sequence $sequence";
		my $script2 = qq(
			create sequence $sequence
			start with $startWith
			cache 20
		);
		if ($opts->{n}) {
			print "$script1 \n";
			print "$script2 \n";
		} else {
			sql->run($script1);
			sql->run($script2);
		}
	}
}


$COMMANDS->{refreshMcSequences} =
  {
   description => "refresh the mail_coll related sequences",
   synopsis => 
   [
    [],
    ["-n", undef ,"don't execute"],
    ],
   routine => \&refreshMailCollSequences,
   };


sub callRefSeq {
	# merge options
	my ($this, $opts, $sequence, $table, $column, $buildRule) = @_;

	$opts->{"s"} = $sequence;
	$opts->{t} = $table;
	$opts->{c} = $column;
	$opts->{b} = $buildRule;

	$this->refreshSequence(undef, $opts);
}
sub refreshMailCollSequences {
	my ($this, $argv, $opts) = @_;

	#$this->callRefSeq($opts, "isb_sq_cta_id", "Isb_Tb_Collection_Target", "cta_id", "centerPrefix");
	$this->callRefSeq($opts, "isb_sq_cta_id", "Snap\$_Isb_Tb_Collection_Ta", "cta_id", "centerPrefix");
	$this->callRefSeq($opts, "ISB_SQ_dor_sequence_num", "snap\$_isb_tb_delivery_orga", "sequence_num", "centerPrefix");
	$this->callRefSeq($opts, "ISB_SQ_mbo_id", "snap\$_isb_tb_mailbox", "mbo_id", "centerPrefix");
	$this->callRefSeq($opts, "ISB_SQ_mct_id", "snap\$_isb_tb_mail_collecti", "mct_id", "centerPrefix");
	$this->callRefSeq($opts, "ISB_SQ_oun_id", "snap\$_isb_tb_org_unit", "oun_id", "centerPrefix");
	$this->callRefSeq($opts, "ISB_SQ_out_id", "snap\$_isb_tb_org_unit_type", "out_id", "centerPrefix");
}

1;







