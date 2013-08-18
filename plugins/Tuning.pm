# ------------------------------------------------------
# NAME
#	Tuning - Provide commands for Tuning
# RESPONSIBILITIES
#
# COLLABORATIONS
#
# History
# ------------------------------------------------------
# $log$
# ------------------------------------------------------

=head1 NAME

Tuning - Provide commands for Tuning

=head1 RESPONSIBILITIES

=head1 COLLABORATIONS

=cut

# $Id: Tuning.pm,v 1.8 2012/12/06 08:54:17 draumart Exp $

package Tuning;
use strict;
use SessionMgr qw(sessionNr sql);
use Plugin;
use Settings;
use Text::Wrap qw(wrap $columns $huge);
use ConIo;
use Misc;
use SQL::Beautify;
use vars qw (
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
	# ----------------------------------------
	$this->{commands} = $COMMANDS;

	bless ($this, $class);

	return $this;
}



# ------------------------------------------------------------
$COMMANDS->{"rollSegs"} =
  {
	  alias => "",
	  description => "print rollback info",
   	  man => qq(
	Without the -u option, this command simply selects from
	dba_rollback_segs. The -u options only gives you rollback
	segments that are currently in use - along with the SIDs.
),
	  synopsis => 
	    [
	     [],
	     ["-u", undef, "print currently used segments with SIDs"],
	     ],
	      routine => \&rollSegs,
    };

sub rollSegs {
	my ($this, $argv, $opts) = @_;

	my $script;

	$script = q(
		select
			s.sid, 
			r.usn,
			r.name rollback,
			p.pid,  
			p.spid,
			nvl(p.username,'NO TRANSACTION') uname,
			p.terminal
		FROM
		v$lock l, v$process p, v$rollname r, v$session s
		WHERE l.sid = s.sid (+)
		and p.addr = s.paddr
		and trunc(l.id1(+)/65536)=r.usn
		and l.type(+) = 'TX'
		and l.lmode(+) = 6
	) if $opts->{"u"};

	$script = q(
		select 
			d.segment_Id id,
			d.Segment_Name name,
			d.tablespace_Name tablespace,
			to_char(initial_Extent/1024,'99990')||' k' ini_ext,
			to_char(min_Extents,'9990') min,
			to_char(v.Extents,'9990') cur,
			to_char(max_Extents,'99990') max,
			to_char(trunc(rssize/(1024*1024)),'9990')||' M' "size",
			to_char(trunc(hwmsize/(1024*1024)),'9990')||' M' "hwm",
			decode (optsize, NULL,'-',to_char(trunc(optsize/(1024*1024)),'990')||' M') "opt",
			d.status
		from 
			dba_rollback_segs d,
			v$rollstat v
		where d.segment_id = v.usn(+)
	)unless $opts->{u};

	sql->run($script);
}

# ------------------------------------------------------------
$COMMANDS->{"ps"} =
  {
	  alias => "",
	  description => "print session information",
	  synopsis => 
	    [
	     [],
	     ["-l", undef, 'print long format'],
	     ["-s", "s1,s2..", 'only show given sids'],
	     ["-p", "pattern", 'only "matching" sessions'],
	     ["-A", undef, 'only active sessions'],
	     ["-U", undef, 'only user sessions'],
	     ["-m", undef, 'print my session'],
	     ["-o", "columns", 'order by these columns'],
	     ],
   man=>qq(
	Display processes and their io (-l option). Allows filtering
	by sids or aribitrary pattern (try ps -p senora). Order output
	by by IO (-l option) or SID (otherwise) or any displayed column
	(-o option)
	),
	      routine => \&ps,
    };

sub ps {
	my ($this, $argv, $opts) = @_;

	my $script;
	my $sidClause = qq (and s.sid in ($opts->{"s"})) if $opts->{"s"};
	my $pattern = "'%".uc($opts->{p})."%'";
	my $patternClause;
	$patternClause = qq( and (
			   upper(s.username) like $pattern
			or upper(s.osuser) like $pattern
			or upper(s.program) like $pattern
		)) if $opts->{p};
	$patternClause = qq (
		and audsid = userenv('sessionid')
	)if $opts->{"m"}; 


	my $activeClause="";
	$activeClause = "and s.status='ACTIVE'" if $opts->{A};

	my $userClause="";
	my $userClause="and b.name is null" if $opts->{U};

	my $orderBy= $opts->{o} ? $opts->{o} :"3*ph_reads + block_gets + 2*consistent_gets";

	# mark my own session
	my $NAME = "decode (audsid,userenv('sessionid'),'SELF',b.name) name";

	if ($opts->{l}) {
		my $orderBy= $opts->{o} ? $opts->{o} :"3*ph_reads + block_gets + 2*consistent_gets";
		$script = " 
			select 
				s.status, 
				s.sid, 
				s.inst_id inst,
				spid, 
				s.serial#, 
				to_char(s.logon_time,'DD.MM HH24:MI') logon, 
				s.username, s.osuser,
				substr(s.program,1,24) program,
				$NAME,
				s.terminal,
				consistent_gets cons_gets, 
				block_gets, physical_reads ph_reads,
				trunc(100*(consistent_gets + block_gets - physical_reads)
				  /(0.1 + consistent_gets + block_gets),2) Hit_ratio,
				trunc(100 * (block_changes + consistent_changes)
				  /(0.1 + block_gets + consistent_gets),2) Undo
		from 	
			gv\$session s,
			gv\$sess_io i,
			gv\$process p,
			gv\$bgprocess b
		where
			s.sid = i.sid
			and s.inst_id = i.inst_id
			and s.paddr = p.addr
			and s.inst_id = p.inst_id
			and s.paddr = b.paddr(+)
			and s.inst_id = b.inst_id(+)
			$sidClause $patternClause $activeClause $userClause
			order by $orderBy";

	} else {
		my $orderBy= $opts->{o} ? $opts->{o} :"SID";
		$script = "
			select 
				status, 
				s.sid,
				s.inst_id inst,
				serial#, 
				$NAME,
				to_char(logon_time,'DD.MM HH24:MI') logon, 
				username, 
				osuser,
				substr(program,1,24) program
			from gv\$session s,
			gv\$bgprocess b
			where 1=1
                        and s.inst_id = b.inst_id(+)
			and s.paddr = b.paddr(+)
			$sidClause $patternClause $activeClause $userClause
		  	order by $orderBy";
	}
	sql->run($script);
}
# ------------------------------------------------------------
$COMMANDS->{"hwm"} =
  {
	  alias => "",
	  description => "get high watermark info on analyzed tables (experimental)",
	  synopsis => 
	    [
	     ["table"],
	     ],
	      routine => \&hwm,
    };

sub hwm {
	my ($this, $argv, $opts) = @_;

	my $script;
	my $table = $argv->[0];
	if (!$table) {
		print "No table.\n";
		return;
	}
	$script = qq {
		select 
			t.last_analyzed, 
			s.blocks,
			t.empty_blocks,
			s.blocks - t.empty_blocks hwm
		from
			dba_segments s,
			dba_tables t
		where
			s.owner = t.owner
		and	s.segment_name = upper('$table')
		and	t.table_name = upper ('$table')
	};
	sql->run($script);
}

# ------------------------------------------------------------
$COMMANDS->{"redo"} =
  {
	  alias => "",
	  description => "print redo log activity",
	  synopsis => 
	    [
	     [],
	     ["-d", "days", 'go back days days'],
	     ["-M", undef, 'group by month'],
	     ["-D", undef, 'group by day'],
	     ["-i", undef, 'show redo increment'],
	     ["-s", "sid", 'show increment of this session (default my session)']
	     ],
	      routine => \&logs,
    };

sub logs {
	my ($this, $argv, $opts) = @_;

	if ($opts->{"i"}) {
		my $sid_clause = "and audsid = userenv('sessionid')";
		if ($opts->{"s"}) {
			$sid_clause = "and e.sid = " . $opts->{"s"};
		}
		my $stmt = qq(
			select 
				s.sid, 
				to_char(sysdate,'HH24:MI:SS') since, 
				value
			from 
				gv\$session e,
				gv\$bgprocess b,
				v\$sesstat s, 
				v\$statname n

			where 1=1
                        		and e.inst_id = b.inst_id(+)
			and e.paddr = b.paddr(+)
			$sid_clause
			and s.sid = e.sid
			and s.statistic# = n.statistic#
			and upper (n.name) = 'REDO SIZE'
		);
		my $redo = sql->runArrayref($stmt);

		my $sid = $redo->[0]->[0];
		my $since = $redo->[0]->[1];
		my $value = $redo->[0]->[2];

		my $last = $this->{redo}->{$sid};

		# formatting
		my $increment = $value - $last->{value};
		my $dimension = "Bytes" ;
		foreach my $d ("kB", "MB", "GB") {
			if ($increment > 5000) {
				$increment = $increment/1024;
				$dimension = $d;
			}
		}

		printf ("redo of SID =%3d since %8s = %5.1f %s\n", 
			$sid, 
			$last->{since} ? $last->{since} : "startup", 
			$increment, $dimension);
		# save values 
		$this->{redo}->{$sid}->{since} = $since;
		$this->{redo}->{$sid}->{value} = $value;
		return;

	}

	my $logFileSize = sql->runArrayref (qq(
		select avg(bytes)/(1024 * 1024) 
		from v\$log
	))->[0]->[0];

	my $timeClause = "where first_time > trunc(sysdate) - $opts->{d}" if $opts->{d};

	my $wCol = "trunc(d,'MM') month,"  if $opts->{M};
	my $wGrp = "group by trunc(d,'MM')" if $opts->{M};
	$wCol = "trunc(d,'DD') day,"  if $opts->{D};
	$wGrp = "group by trunc(d,'DD')" if $opts->{D};

	  sql->run (qq(
	 	select 
			$wCol
			min(n) "min/day", 
			max(n) "max/day", 
			trunc(avg(n)) "avg/day",

			$logFileSize * trunc(min(n)) "min Mb/day", 
			$logFileSize * trunc(max(n)) "max Mb/day", 
			$logFileSize * trunc(avg(n)) "avg Mb/day" 
		from
			(
			select trunc(first_time, 'DD') d , count(*) n
			from V\$LOG_HISTORY
			$timeClause
			group by trunc(first_time, 'DD')
			)
		$wGrp
	  ));
}


# ------------------------------------------------------------
$COMMANDS->{"logminer"} =
  {
	  alias => "",
	  description => "show contents of single logfile",
	  synopsis => 
	    [
	     [],
	     ["-d", "dir", "directory of logfiles and dictionary.ora"],
	     ["-D", undef, "create dictionary.ora"],
	     ["-f", "filename", 'the logfile to display'],
	     ["-v", undef, 'print full SQL instead of summary'],
	     ],
	      routine => \&logminer
    };

sub logminer {
	my ($this, $argv, $opts) = @_;
	my $dir = $opts->{d} or return; 
	my $filename = $opts->{f};

	return unless ($filename || $opts->{D});

	if ($opts->{D}) {
		sql->run(qq(begin
		DBMS_LOGMNR_D.BUILD(DICTIONARY_FILENAME =>'dictionary.ora', DICTIONARY_LOCATION => '$dir');
		end;));
		return;
	}

	sql->run(qq(
	begin 
		DBMS_LOGMNR.ADD_LOGFILE(LogFileName => '$dir/$filename',
   			Options => dbms_logmnr.NEW);
		DBMS_LOGMNR.START_LOGMNR(
   			DictFileName =>'$dir/dictionary.ora');
	end;));



	if ($opts->{v}) {
		sql->run(qq(
			SELECT sql_redo
			FROM V\$LOGMNR_CONTENTS
		));
	} else {
      		sql->run(qq(
			SELECT operation, seg_name, count(*)
			FROM V\$LOGMNR_CONTENTS
			group by operation, seg_name));

	}

	sql->run(qq(
		BEGIN DBMS_LOGMNR.END_LOGMNR(); END;
	));

}

# ------------------------------------------------------------
$COMMANDS->{"sesstat"} =
  {
	  alias => "ss",
	  description => "print session statistics",
	  synopsis => 
	    [
	     [],
	     ["-s", "sid", 'The sid to stat'],
	     ["-p", "pattern", 'pattern to match the statname'],
	     ["-a", undef, 'also show values=0'],
	     ["-l", "limit", 'only show values > limit'],
	     ],
	      routine => \&sesstat,
    };

sub sesstat {
	my ($this, $argv, $opts) = @_;
	settings->push;
	columnFormat->set("STATNAME","a40");
	my $script;

	my $sidClause = qq(and SID in ($opts->{"s"})) if $opts->{"s"};
	my $nameClause = "and upper(name) like '%" . uc($opts->{p}) . "%'" if $opts->{p};
	my $nullClause = "and value != 0" unless $opts->{a};
	$nullClause = "and value > $opts->{l}" if $opts->{l};
	my $script = qq{
		select sid, name statname , value
		from
			v\$sesstat s, v\$statname n
		where
			s.statistic# = n.statistic#
			$sidClause $nameClause $nullClause
		order by value desc
		};
	sql->run($script);
	settings->pop;
}

# ------------------------------------------------------------
$COMMANDS->{"jobs"} =
  {
	  alias => "",
	  description => "print job information",
	  synopsis => 
	    [
	     [],
	     ["-l", undef, 'Print long format'],
	     ["-p", "pattern", 'Print only jobs with matching "what"'],
	     ["-r", undef, 'Print only hanging, running or broken jobs'],
	     ["-d", "days",'delay all matching jobs by days']
	     ],
	      routine => \&jobs,
    };

sub jobs {
	my ($this, $argv, $opts) = @_;

	settings->push;
	columnFormat->set("WHAT","a40");

	my $script;
	my $what = ",what" if $opts->{l};
	my $patternClause = "and upper(j.what) like '%" . uc($opts->{p}) ."%'" if $opts->{p};
	my $rClause = qq ( and (
		r.sid is not null 
		or j.last_date < sysdate - 1
		or j.broken='Y'
		)) if $opts->{r};
	if ($opts->{d}) {
		my $jobs = sql->runArrayref(qq(
			select job, substr(what, 1,40) what, to_char(next_date + $opts->{d},'DD.MM.YYYY HH24:MI')
			from all_jobs j
			where 1=1
			$patternClause
		));
		my $i=0;
		while (my $job = $jobs->[$i]) {
			print "delaying $job->[0] $job->[1] to $job->[2]\n";
			sql->run("begin 
					dbms_ijob.next_date($job->[0],to_date('$job->[2]','DD.MM.YYYY HH24:MI')); 
				end;");
			$i++;
		}
	}else {
		$script = qq (
			select
				j.job,
				j.broken br,
				decode(
					trunc((sysdate - j.last_date)),
					0,
					NULL,
					trunc(sysdate - j.last_date)
				)  hung,
				r.sid,
				s.physical_reads ph_reads,
				to_char(j.last_date,'DD.MM. HH24:MI') last_date,
				to_char(r.this_date,'DD.MM. HH24:MI') this_date,
				to_char(j.next_date,'DD.MM. HH24:MI') next_date
				$what
			from
				dba_jobs j,
				dba_jobs_running r,
				v\$sess_io s
			where
				j.job = r.job(+)
			and	s.sid(+) = r.sid $patternClause $rClause
		);

		sql->run($script);
		}
	settings->pop;
}


# ------------------------------------------------------------
$COMMANDS->{"sched"} =
  {
	  alias => "",
	  description => "print scheduler information",
	  synopsis => 
	    [
	     [],
	     ["-l", undef, 'Print long format'],
	     ["-p", "pattern", 'Find only matching jobs'],
	     ],
	      routine => \&sched,
    };

sub sched {
	my ($this, $argv, $opts) = @_;
	my $projection = $opts->{l} ? 
	    "enabled,Owner,job_name,job_type,job_action, state, failure_count, last_start_date" :
	      "enabled,Owner,job_name,job_type,job_action" ;
	my $patternClause;
	if (my $pattern = uc($opts->{p})) {
		$patternClause = "where upper(job_Name) like '%$pattern%' or upper(job_action) like '%$pattern%'";
	}

	sql->run(qq(
		select $projection from dba_scheduler_jobs
		$patternClause
	));
}

# ------------------------------------------------------------
$COMMANDS->{"xqueries"} =
  {
	  alias => "xq",
	  description => "show most expensive quieries",
	  synopsis => 
	    [
	     [],
	     ["-n", "rows", 'show top n rows'],

	     ["-i", undef, 'check increase agains copy of v$sqlarea'],
	     ["-s", undef, 'create a snapshot of v$sqlarea for -i'],

	     ["-p", "pattern", 'print only statements matching pattern'],

	     ["-R", undef, 'order by reads'],
	     ["-X", undef, 'order by reads/xl'],
	     ["-N", undef, 'order by execs + loads'],
	     ],
   	  man => q(

	Xqueries gives you the most expensive queries since the
	database was started. It does this by looking at the
	statistics in v$sqlarea. The 'disk reads' are considered the
	main cost factor.

	The -s and -i options create a snapshot of v$sqlarea and
	consider the increase of the statistics instead of their
	absolute values. This allows to look at more recent times. 

	To check very recent times, you may prefer the 'profile'
	command.

),

	      routine => \&mostExpensiveQueries,
    };

sub mostExpensiveQueries {
	my ($this, $argv, $opts) = @_;

	my $script;
	my $orderClause = "";
	my $matchClause = "";


	if ($opts->{"s"}) {
		sql->run("drop table senora_sqlarea");
		sql->run("create table senora_sqlarea as select * from v\$sqlarea");
		return;
	}
	# get the limits and ordering right

	settings->push;
	columnFormat->set("STATEMENT","a40");


	$orderClause .= ", reads" if $opts->{R};
	$orderClause .= ', "rds/xl"' if $opts->{X};
	$orderClause .= ', (0.001 + a.executions + a.loads -1)'if $opts->{N};
	$orderClause =~ s/^,//;

	$matchClause = "and upper(sql_text) like '%$opts->{p}%'" if $opts->{p};

	$orderClause = '"rds/xl"' unless $orderClause;
	$orderClause .= " desc" if $orderClause;
	my $maxrows = $opts->{n} ? $opts->{n} : 10;
	print "Order by $orderClause (top $maxrows)\n";

	# get incremental costs
	my $refQuery = qq<(
	select 
	new.SQL_TEXT  ,
        new.parsing_user_id parsing_user_id,
        new.command_type command_type,
	new.SHARABLE_MEM       - nvl(ref.SHARABLE_MEM,0)   SHARABLE_MEM       ,
	new.PERSISTENT_MEM     - nvl(ref.PERSISTENT_MEM,0) PERSISTENT_MEM     ,
	new.RUNTIME_MEM        - nvl(ref.RUNTIME_MEM,0)    RUNTIME_MEM        ,
	new.SORTS              - nvl(ref.SORTS,0)          SORTS              ,
	new.EXECUTIONS         - nvl(ref.EXECUTIONS,0)     EXECUTIONS         ,
	new.LOADS              - nvl(ref.LOADS,0)          LOADS              ,
	new.INVALIDATIONS      - nvl(ref.INVALIDATIONS,0)  INVALIDATIONS      ,
	new.PARSE_CALLS        - nvl(ref.PARSE_CALLS,0)    PARSE_CALLS        ,
	new.DISK_READS         - nvl(ref.DISK_READS,0)     DISK_READS         ,
	new.BUFFER_GETS        - nvl(ref.BUFFER_GETS,0)    BUFFER_GETS        ,
	new.ROWS_PROCESSED     - nvl(ref.ROWS_PROCESSED,0) ROWS_PROCESSED     
	from
		senora_sqlarea ref,
		v\$sqlarea new
	where
		new.address = ref.address(+)
	)>;

	my $ref = 'v$sqlarea';
	if ($opts->{i}) {
		$ref = $refQuery;
		print "incremental\n";
	}
	#----------------------------------------
		$script = qq(
			select
				b.username username,
				a.disk_reads reads,
				a.executions exec,
				a.loads loads,
				trunc(a.disk_reads / (0.001 + a.executions + a.loads -1))
					"rds/xl",
				a.command_type cmd,
				rtrim(a.sql_text,chr(0)) statement
			from
				$ref a,
				dba_users b
			where
				a.parsing_user_id = b.user_id
				$matchClause
			order by
				$orderClause
		);
		sql->prepare($script);
		sql->execute;
		sql->fetch($maxrows);
		return;
	settings->pop;
}

# ------------------------------------------------------------
$COMMANDS->{"cstatement"} =
  {
	  alias => "cs",
	  description => "show (first lines) of the current statements of sessions",
	  synopsis => 
	    [
	     [],
	     ["-u", "user", 'limit to a given user'],
	     ["-s", "s1,s2..", 'limit to a given SIDs'],
	     ["-P", undef, 'show previous statement'],
	     ["-p", undef, 'show execution plan'],
	     ["-o", undef, 'show accessed objects of execution plan'],
	     ["-S", undef, 'show statistics of statement'],
	     ["-b", undef, 'show bind parameters of statement'],
	     ["-b", undef, 'show bind parameters of statement'],
	     ["-v", undef, 'show beautified SQL, not just the first lines'],
	     ],
	      routine => \&currentStatement,
    };

sub currentStatement {
	my ($this, $argv, $opts) = @_;

	my $clause;
	my $script;

	if ($opts->{u}) {
		$clause = 
		  "\n\t\tand (username = '" 
		    . uc($opts->{u}) 
		      ."' or osuser= '" 
			. uc($opts->{u}) ."')";
	}
	if ($opts->{"s"}) {
		$clause = 
		  qq(\n\t\tand sid in ($opts->{s}));
	}
	my $sql_address = $opts->{P} ? "prev_sql_addr" : "sql_address";
	my $sql_child_number = $opts->{P} ? "prev_child_number" : "sql_child_number";
	# ------------------------------------------------------------
	if ($opts->{S}) {
		$script = qq(
			select 
			     se.sid, 
			     se.serial#,
			     se.inst_id,
			     username, 
			     osuser, 
			     rawtohex($sql_address),
			     $sql_child_number,
			     sa.executions,
			     sa.disk_reads,
			     sa.buffer_gets
			from 	
			     gv\$session se,
			     gv\$sqlarea sa
			where
			     1=1 $clause
			     and se.sql_address=sa.address
			     and se.inst_id = sa.inst_id
			     order by sid
		    );	     
		} else {     
		$script = qq(
			select 
			     se.sid, 
			     se.serial#,
			     se.inst_id,
			     username, 
			     osuser, 
			     rawtohex($sql_address),
			     $sql_child_number
			from 	
			     gv\$session se
			where
			     1=1 $clause
			     order by sid
		    );
		}
	my $sth = sql->prepare ($script) or return;
	$sth->execute;
	while (my $row = $sth->fetchrow_arrayref) {
		if ($row->[5] cmp "0") {
			print "------------------\n";
			print "SID=$row->[0] SERIAL=$row->[1] USERNAME=$row->[2] OSUSER=$row->[3]\n";
			print "Execs=$row->[7], DiskReads=$row->[8], BufferGets=$row->[9]\n" if $opts->{S};
			# ------------------------------------------------------------
			printStatementOfAddress ($row->[5], $opts);
			printPlanOfAddress($row->[5], $row->[6], $opts) if $opts->{p};
			printBindsOfAddress($row->[5], $row->[6]) if $opts->{b};
		} else {
			print "No statement executing.\n";
		}
	}

	return;
}

sub printStatementOfAddress{
    my ($address, $opts) = @_;
		my $script = qq (
			select sql_text current_statement
			from gv\$sqltext_with_newlines
			where address = hextoraw('$address')
			order by piece
		);
		my $sth = sql->prepare ($script);
		$sth->execute;
		my $text = "";
		while (my $row = $sth->fetchrow_arrayref) {
			$row->[0]=~s/·//;
			$text .= $row->[0];
		}
		my $beauty = SQL::Beautify->new(query=>$text)->beautify();
		if ($opts->{v}) {
			print "$beauty";
		} else {
			$beauty =~ s/[\n\t ]+/ /g;
			my $firstlines = substr($beauty,0,40);
			print "  $firstlines";
			if (length($firstlines) == length($beauty)) {
				print ";\n";
			} else {
				print " ...\n";
			}
		}
}

sub printBindsOfAddress {
    my($address, $child) = @_;
    print "\nBind parameters:";
    sql->run(qq(
    	select position,name, value_string 
	from v\$sql_bind_capture 
	where address= hextoraw('$address') and child_number=$child
	order by name
    ));
}

sub printPlanOfAddress {
    my($address, $child, $opts) = @_;
    print "Execution plan of $address\n"; 
    sql->run(qq(
	select rpad(lpad('*',level,'*'),12,' ')||lpad (' ', 4*level, '   |')|| operation || ' ' || Options || ' ' ||
                object_name "Operation",
	optimizer, cost, cardinality, bytes, other_tag
        from gv\$sql_plan
        where 1=1
	start with 
	      address=hextoraw('$address') and child_number=$child and id=0
        connect by 
		prior id = parent_id
		and prior address=address
		and prior child_number = child_number
		and prior plan_hash_value = plan_hash_value
		and prior inst_id = inst_id

	));
	printPlanHash("gv\$sql_plan where address='$address'");
	if ($opts->{o}){
		printPlanObjects("gv\$sql_plan", "where address=hextoraw('$address') and child_number=$child");
	}
}

sub createPlanTable {
	my $stmt = q(create table PLAN_TABLE
	(
		statement_id    varchar2(30),
		timestamp       date,
		remarks         varchar2(80),
		operation       varchar2(30),
		options         varchar2(30),
		object_node     varchar2(128),
		object_owner    varchar2(30),
		object_name     varchar2(30),
		object_instance numeric,
		object_type     varchar2(30),
		optimizer       varchar2(255),
		search_columns  number,
		id              numeric,
		parent_id       numeric,
		position        numeric,
		cost            numeric,
		cardinality     numeric,
		bytes           numeric,
		other_tag       varchar2(255),
		partition_start varchar2(255),
		partition_stop  varchar2(255),
		partition_id    numeric,
		other           long           
	 ));
	sql->run($stmt);
}
# ------------------------------------------------------------
sub printPlanObjects{
    my ($from, $where) = @_;

	my $rows = sql->runArrayref(qq(
	   select distinct ''''||object_name||''''
	   from $from pt
	   $where
	   and (operation like 'TABLE%' or operation like 'INDEX%')
	));

    my @oo;
    foreach my $o (@$rows) { push (@oo, $o->[0]);}
    my $objects = "(".join (",", @oo).")";
	sql->run (qq(
		select * from (
		select table_name, partition_name, num_rows, last_analyzed
		from user_tab_partitions
		where table_name in $objects
		union all
		select table_name, NULL, num_rows, last_analyzed
		from user_tables
		where table_name in $objects
		union all
		select index_name, partition_name, num_rows, last_analyzed
		from user_ind_partitions
		where index_name in $objects
		union all
		select index_name, NULL, num_rows, last_analyzed
		from user_indexes
		where index_name in $objects
		)
		order by 1, 2, 3
	 ));

}

# ------------------------------------------------------------
$COMMANDS->{"xplain"} =
  {
	  alias => "",
	  description => "explain plan for statement or file",
	  synopsis => 
	    [
	     [],
	     ["-f", "file", 'take statement from file'],
	     ["-e", undef, 'echo statement'],
	     ["-v", undef, 'print access predicates too'],
	     ["-q", undef, "don't print the plan itself"],
	     ["-o", undef, 'print the accessed objects'],
	     ["-t", undef, 'print the accessed tables'],
	     ["-c", undef, 'only create plan table'],
	     ],
	      routine => \&explain,
    };

# ------------------------------------------------------------
sub explain {
	my ($this, $argv, $opts) = @_;

	my $stmt;
	if ($opts->{c}) {
		$this->createPlanTable;
		return;
	}
	if ($opts->{f}) {
		my $fd = FileHandle->new($opts->{f});
		if (!$fd) {
			print "cannot open file.\n";
			return;
		}
		my @stmt = <$fd>;
		close ($fd);
		$stmt = "@stmt";
	} else {
		shift;
		$stmt = "@$argv";
	}
	$stmt =~ s/;\s*$//;
	print $stmt if $opts->{e};
	my $sth=sql->prepare("delete from plan_table where statement_id='99'")->execute;
	my $script = qq (
		explain plan set statement_id = '99'
		for $stmt
	);
	my $sth=sql->prepare("$script");
	return unless $sth;
	$sth->execute;

	my $accessPredicates = q(||'  ('|| replace(access_predicates||filter_predicates,'"','')||')') if $opts->{v};
	my $script = qq(
	select lpad (' ', 4*level, '   |')|| operation || ' ' || Options || ' ' ||
                object_name $accessPredicates "Operation",
	optimizer, cost
        from plan_table
        where statement_id = '99'
        connect by prior id = parent_id
                and statement_id = '99'
        start with id = 0  and statement_id = '99'   
	);
	sql->run($script) unless $opts->{"q"};

	printPlanHash("plan_table where statement_id='99'");

	if ($opts->{"o"}) {
	   	printPlanObjects("plan_table", "where statement_id='99'");
	}
	if ($opts->{"t"}) {
		sql->run(qq(
		select table_name, count(*) accesses from (
		select object_name table_name from plan_table where statement_id='99' and object_type like 'TABLE%'
		    union all
		select table_name  from user_indexes where index_name in 
			(select object_name from plan_table where statement_id ='99')
		) group by table_name
		));
	}

	# cleanup
	#my $sth=sql->prepare("delete from plan_table where statement_id='99'")->execute;
	return;
}

sub printPlanHash {
    my ($fromWhere) = @_;
	# create a hash-values to compare plans and get the accessed objects
	my $rows = sql->runArrayref(qq(
		select id||operation||options||object_name txt, cost 
		from $fromWhere
		order by id));
	my $hv = 0;
	my $objects = {};

	foreach my $row (@$rows) {
		my $val = $row->[0];
		$val =~ tr/A-Za-z /0-90-90-90-9/;
		# work in 6digit packets
		for (my $i=0; $i<= length($val); $i = $i+6) {
			$hv += ($i+1)*substr($val, $i, 6);
		}

	}

	printf ("Plan hash = %d; cost = %d\n", $hv%10000, $rows->[0]->[1]);
}

# ------------------------------------------------------------
$COMMANDS->{"validate"} =
  {
   alias => undef,
   description => "validate an indexes or tables",
   synopsis => 
   [
    ["pattern"],
   ],
   man=>qq(
	Runs analyze table validate structure cascade for tables and
	analyze index validate structure for indexes. For indexes
	the contents of index_stats is printed.
),
   routine => \&validate
  };

# ------------------------------------------------------------
sub validate {
	my ($this, $argv, $opts) = @_;

	my ($typePattern, $namePattern) = processPattern($argv);

	my $sth_objects = sql->prepare (qq(
		select * from (
			select 'INDEX', index_name
			from user_indexes
			where index_name like $namePattern
			and 'INDEX' like $typePattern
			union
			select 'TABLE', table_name
			from user_tables
			where table_name like $namePattern
			and 'TABLE' like $typePattern
		) 
	));
	$sth_objects->execute;
	while (my $row = $sth_objects->fetchrow_arrayref) {

		my $otype = $row->[0];
		my $oname = $row->[1];
		printf ("%-8s %-32s", $otype, $oname);
		if ($otype =~ /TABLE/) {
			validateTable ($oname);
		}
		elsif ($otype =~ /INDEX/) {
			validateIndex ($oname);
		}
		else {
			print ("Cannot validate a $otype \n");
		}
	}
}

# ------------------------------------------------------------
sub validateIndex {
	my ($index) = @_;

	sql->run("analyze index $index validate structure");

	my $sth = sql->prepare("select * from index_stats");
	$sth->execute;
	my $row = $sth->fetchrow_arrayref ;
	my $col=0;
	while (1) {
		return unless $sth->{NAME}->[$col];
		printf ("%-20s %s\n",
			$sth->{NAME}->[$col],
			$row->[$col]
		       );
		$col++;
	}
}

# ------------------------------------------------------------
sub validateTable {
	my ($table) = @_;
	sql->run("analyze table $table validate structure cascade");
}

# ------------------------------------------------------------
$COMMANDS->{"kept"} =
  {
	  alias => "",
	  description => "show kept (pinned) code",
	  synopsis => 
	    [
	     [],
	     ["-p", "pattern", 'restrict by pattern'],
	     ["-n", undef, 'show non pinned'],
	     ],
	      routine => \&kept
    };

# ------------------------------------------------------------
sub kept {
	my ($this, $argv, $opts) = @_;

	my $index = $argv->[0];
	my $matchClause = "and upper(name) like '%".uc($opts->{p})."%'" if $opts->{p};
	my $pinClause = "kept = 'YES'";

	settings->push;
	columnFormat->set("NAME","a40");

	$pinClause = "kept != 'YES'" if $opts->{n};
	sql->run("
		SELECT name, type, kept
  		FROM   v\$db_object_cache
  		WHERE $pinClause $matchClause");
	settings->pop;
}

# ------------------------------------------------------------
$COMMANDS->{"space"} =
  {
	  alias => "",
	  description => "show tablespace and file stats",
	  synopsis => 
	    [
	     [],
	     ["-t", undef, 'show tablespace stats'],
	     ["-d", undef, 'show datafile stats'],
	     ["-D", undef, 'show resizable datafiles'],
	     ["-F", undef, 'show largest free chunk per tbspc'],
	     ["-b", "blksz", 'assume blocksize for -D'],
	     ["-e", "n", 'show top n extent consumers'],
	     ["-s", "n", 'show top n space consumers'],
	     ["-X", undef, 'show objects that cannot allocate next extent'],
	     ["-o", "columns", 'order by these columns'],
	     ],
	      routine => \&space
    };

# ------------------------------------------------------------
sub space {
	my ($this, $argv, $opts) = @_;

	my $tsClause;
	if ($opts->{T}) {
		my $ts = uc($opts->{T});
		$tsClause = "and d.tablespace_name like '%$ts%'";
	}
	my $orderBy= $opts->{o} ? $opts->{o} : "1";
	# ----------------------------------------
	if ($opts->{t}) {
	sql->run (qq(
	SELECT 
		d.tablespace_name "Name", 
		d.status "Status",
		TO_CHAR((a.bytes / 1024 / 1024),'9999,990') "Size/MB",    
		TO_CHAR((DECODE(f.bytes, NULL, 0, f.bytes) / 1024 / 1024),'9999,990') "Free/MB",
		TO_CHAR((a.bytes - (DECODE(f.bytes, NULL, 0, f.bytes))) / 1024 / 1024,'9999,990') "Used/MB",
		to_char(trunc(100 * (a.bytes - DECODE(f.bytes, NULL, 0, f.bytes))/(a.bytes)),'9999') "used%"
		FROM 
			sys.dba_tablespaces d, 
			sys.sm\$ts_avail a, 
			sys.sm\$ts_free f
		WHERE 
			d.tablespace_name = a.tablespace_name 
		AND f.tablespace_name (+) = d.tablespace_name
		$tsClause
		order by $orderBy
	));
	return;
}
	# ----------------------------------------
	
	if ($opts->{d}) {
	sql->run (qq(
	   select
		d.file_id, file_name, 
		to_char (bytes/(1024*1024),'9999,990') "size/MB",
		to_char(nvl(xfree/(1024*1024),0),'9999,990') "Free/MB",
		to_char(nvl((bytes-xfree)/(1024*1024),0),'9999,990') "used/MB", 
		to_char(nvl(trunc(100*(bytes-xfree)/(bytes)),100),'9999,990') "used%",
		-- to_char(xused,'99,990') "used/MB", 
		to_char(trunc (maxbytes/(1024*1024)),'9999,990') "max/MB",
		tablespace_name tablespace
	from
		(
		select 
			file_name, 
			tablespace_name, 
			file_id, 
			maxbytes,
			bytes
		from dba_data_files
		group by file_name, file_id, tablespace_name, maxbytes, bytes
		) d,
		(
		select 
			file_id, 
			sum(bytes) xfree
		from dba_free_space
		group by file_id
		) f
	where d.file_id = f.file_id
	order by $orderBy
	));
	return
}
	# ----------------------------------------
	if ($opts->{F}) {
	my $blksz = $opts->{b} ? $opts->{b} : sql->blockSize;
	print "assuming blocksize of $blksz\n";
	sql->run (qq(
		select 
			tablespace_name, 
			trunc((max(blocks) * $blksz)/(1024*1024))|| ' MB' largest 
		from dba_free_space 
		group by tablespace_name
	));
	return;
}

	# ----------------------------------------
	my $orderBy= $opts->{o} ? $opts->{o} :"reclaim";

	if ($opts->{D}) {
	my $blksz = $opts->{b} ? $opts->{b} : sql->blockSize;
	print "assuming blocksize of $blksz\n";
	sql->run (qq(
		select
		       	file_name,
			to_char(trunc(high/(1024*1024)),'9999')||'MB' high,
			to_char(trunc(fileSize/(1024*1024)),'9990')||'MB' fileSize,
			to_char(trunc((fileSize-high)/(1024*1024)),'9990')||'MB' reclaim
		from (
			select 
				file_name, 
				max(block_id*$blksz+e.bytes) high,
				f.bytes fileSize
			from 
				dba_extents e,
				dba_data_files f
			where
				f.file_id = e.file_id
			group by 
				file_name, f.bytes
		)
		order by $orderBy
	));

	return;
}
	# ----------------------------------------
	if ($opts->{e}) {
	sql->prepare (qq(
			select 
				segment_type, 
				owner,
				segment_name,
				trunc(sum(e.bytes)/(1024*1024)) MB,
				count(*) extents
			from 
				dba_extents e
			group by segment_type, owner, segment_name
			order by 5 desc
	));
	sql->execute && sql->fetch($opts->{e});
	return;
}
	# ----------------------------------------
	if ($opts->{"s"}) {
	sql->prepare (qq(
			select /* +xFIRST_ROWS */
				segment_type, 
				owner,
				segment_name,
				trunc(sum(e.bytes)/(1024*1024)) MB,
				count(*) extents
			from 
				dba_extents e
			group by segment_type, owner, segment_name
			order by 4 desc
	));
	sql->execute && sql->fetch($opts->{"s"});
	sql->finish;
	return;
}
	# ----------------------------------------
	if ($opts->{X}) {
		my $view = setting->get('ddView');
		print "Objects that could not allocate next extent:\n";
		sql->run(qq(
		SELECT s.segment_type, s.segment_name, s.next_extent/(1024) "next/kB", Tablespace_name
		FROM ${view}_segments s
		WHERE s.next_extent  >
		 ( SELECT MAX( f.bytes ) 
			FROM ${view}_free_space f
			WHERE f.tablespace_name = s.tablespace_name 
		)
	));
	}
}


# ------------------------------------------------------------
$COMMANDS->{"resize"} =
  {
	  alias => "",
	  description => "resize datafiles ",
	  synopsis => 
	    [
	     [],
	     ["-m", "Mbytes", 'limit to at least Mbytes space reclaimes (50)'],
	     ["-f", undef, 'force: dont ask'],
	     ["-b", "blksz", 'assume blocksize for -D'],
	     ],
	      routine => \&resize
    };

# ------------------------------------------------------------
sub resize {
	my ($this, $argv, $opts) = @_;

	my $megabytes = $opts->{"m"} ? $opts->{"m"} : 50;
	my $blksz = $opts->{b} ? $opts->{b} : sql->blockSize;
	print "assuming blocksize of $blksz\n";

	my $script= (qq(
		select 
			file_name, 
			trunc(max(block_id*$blksz+e.bytes)/(1024*1024)) used, 
			trunc(f.bytes/(1024*1024)) fileSize
		from 
			dba_extents e,
			dba_data_files f
		where
			f.file_id = e.file_id
		group by file_name,  trunc(f.bytes/(1024*1024))
	));

	my $sth = sql->prepare($script);
	$sth->execute;
	while (my $row = $sth->fetchrow_arrayref) {
		# prudently add a used megabyte
		$row->[1]++;
		if (($row->[2] - $row->[1]) > $megabytes) {
			if (!$opts->{f}) {
				print "resize $row->[0] from $row->[2] to $row->[1]\n";
				printf ("  (will reclaim %d MB) ?", ($row->[2] - $row->[1]));
				my $answer = conIn;
				print "answer = $answer\n";							 

				next if ($answer !~ /y/i);
			}
			sql->run("alter database datafile '$row->[0]' resize $row->[1] M");
		}
	}
}



# ------------------------------------------------------------
$COMMANDS->{"waits"} =
  {
	  alias => "",
	  description => "show what sessions are waiting for",
	  synopsis => 
	    [
	     [],
	     ["-s", "s1,s2..", 'restrict to SIDs'],
	     ['-q',undef,"don't show idle events"],
	     ],
	      routine => \&waits
    };

# ------------------------------------------------------------
sub waits {
	my ($this, $argv, $opts) = @_;
	settings->push;
	columnFormat->set("TEXT", "a24");
	my $sidClause;
	if (my $sid=$opts->{"s"}) {
		$sidClause = qq(where sid in ($sid));
	}
	my $noIdleClause;

	# provided by chipach
	if ($opts->{"q"}) {
		if($sidClause) {
			$noIdleClause = "and ";
		} else {
			$noIdleClause = "where ";
		}
		$noIdleClause .= <<EOF;
event not in ('lock element cleanup',
			  'pmon timer',
			  'rdbms ipc message',
			  'smon timer',
			  'SQL*Net message from client',
			  'SQL*Net break/reset to client',
			  'SQL*Net message to client',
			  'SQL*Net more data to client',
			  'dispatcher timer',
			  'Null event',
			  'io done',
			  'parallel query dequeue wait',
			  'parallel query idle wait - Slaves',
			  'pipe get',
			  'PL/SQL lock timer',
			  'slave wait',
			  'virtual circuit status',
			  'WMON goes to sleep'
			  )

EOF
	}

	sql->run("
	select
		SID,
		inst_id inst,
		EVENT ,
		P1TEXT ||' '||
		P1     ||' '||
		decode (P2TEXT,NULL,NULL,
			P2TEXT ||' '||        
			P2     ||' ')        
		||
		decode (P3TEXT,NULL,NULL,
			P3TEXT ||' '||        
			P3     ||' ')        
		text,
		WAIT_TIME wtime,   
		SECONDS_IN_WAIT secs_in_wt,
		STATE
	from gv\$session_wait
	$sidClause
        $noIdleClause
	");   
	settings->pop;
}

# ------------------------------------------------------------
$COMMANDS->{"locks"} =
  {
	  alias => "",
	  description => "show sessions and the objects the are waiting for",
	  synopsis => 
	    [
	     [],
	     ],
	      routine => \&locks
    };

# ------------------------------------------------------------
sub locks {
	my ($this, $argv, $opts) = @_;

	sql->run(qq(
		    select
		    holding.sid hold_sid,
		    waiting.sid wait_sid,
		    holding.id1,
		    holding.id2,
		    holding.type,
		    decode(holding.lmode, 
			   0, 'None',           /* Mon Lock equivalent */
			   1, 'Null',           /* N */
			   2, 'Row-S (SS)',     /* L */
			   3, 'Row-X (SX)',     /* R */
			   4, 'Share',          /* S */
			   5, 'S/Row-X (SSX)',  /* C */
			   6, 'Exclusive',      /* X */
			   to_char(holding.lmode)) mode_held,
		    decode(waiting.request,
			   0, 'None',           /* Mon Lock equivalent */
			   1, 'Null',           /* N */
			   2, 'Row-S (SS)',     /* L */
			   3, 'Row-X (SX)',     /* R */
			   4, 'Share',          /* S */
			   5, 'S/Row-X (SSX)',  /* C */
			   6, 'Exclusive',      /* X */
			   to_char(waiting.request)) mode_requested
		    from
		    (
		     select 
		     sid,
		     type,
		     lmode,
		     id1,
		     id2
		     from
		     gv\$lock h
		     where	
		     1=1
		     and	h.block >= 1
		     and	h.lmode != 0
		     and	h.lmode != 1
		    ) holding,
		    gv\$lock waiting
		    where
		    1=1
		    and	waiting.request != 0
		    and	waiting.type = holding.type
		    and	waiting.id1 = holding.id1
		    and	waiting.id2 = holding.id2
		   ));
}

# ------------------------------------------------------------
$COMMANDS->{"show parameter"} =
  {
	  alias => "sp",
	  description => "show init.ora parameter",
	  synopsis => 
	    [
	     ["pattern"],
	     ],
	      routine => \&showParameter
    };

# ------------------------------------------------------------
sub showParameter {
	my ($this, $argv, $opts) = @_;

	my $pattern = "'%".uc($argv->[0])."%'";
	sql->run(
		  "select
			NAME,       
			TYPE,       
			VALUE,      
			ISDEFAULT,  
			ISSES_MODIFIABLE,
			ISSYS_MODIFIABLE,
			ISMODIFIED, 
			ISADJUSTED, 
			DESCRIPTION
		from v\$parameter
		where upper(name) like $pattern
		order by name
		");
}


# ------------------------------------------------------------
$COMMANDS->{"kill"} =
  {
	  alias => "",
	  description => "kill a session",
	  synopsis => 
	    [
	     ["sid"],
	     ],
	      routine => \&kill
    };

# ------------------------------------------------------------
sub kill {
	my ($this, $argv, $opts) = @_;

	my $sid = $argv->[0];
	my $sth=sql->prepare(qq(
		select sid, serial#
		from v\$session
		where sid=$sid
	));
	$sth->execute || return;
	if ( my $row=$sth->fetchrow_arrayref()) {
		my $serial = $row->[1];

		sql->run("alter system kill session '$sid,$serial'");
		sql->run("select sid, status, username, osuser, program 
				from v\$session where sid=$sid");
	} else {
		print "No such sid '$sid'.\n";
	}
}

# ------------------------------------------------------------
$COMMANDS->{"stats"} =
  {
	  alias => "",
	  description => "Wrapper around dbms_stats. Replaces 'analyze'",
	  synopsis => 
	    [
	     [],
	     ["-c", undef, 'create STATTAB'],

	     ["-g", undef, "gather stats"],
	     ["-d", undef, 'delete stats'],
	     ["-i", undef, "import stats"],
	     ["-e", undef, "export stats"],

	     ["-S", "stattab", "The stattab to use"],
	     ["-I", "statid", "The statid to use"],

	     ["-T", "table name", "specify table instead of schema"],

	     ["-l", undef, "gather in more detail"], 
	     ["-p", "object", "show statistics of objects"],
	     ["-P", "object", "show summary statistics of objects if doubtful"],
	     ],
	      routine => \&stats,
    };


sub checkStattab {
	my ($stattab, $statid) = @_;

	if ($stattab eq 'NULL') {
		print "** Error: stattab not set\n";
		return 0;
	}
	if ($statid eq 'NULL') {
		print "** Warning: STATID is null **\n";
	}
	return 1
}
sub stats {
	my ($this, $argv, $opts) = @_;
	my $stattab = $opts->{S} ? "'".uc($opts->{S})."'" : 'NULL';
	my $statid = $opts->{I} ? "'".$opts->{I}."'" : 'NULL';
	my $table = $opts->{T} ? "'".uc($opts->{T})."'" : undef;


	my $estimatePercent;
	my $methodOpt;
	my $granularity;
	if ($opts->{l}) {
		$granularity="Granularity=>'ALL',";
	}else {
		$estimatePercent='Estimate_Percent=>5,';
		$methodOpt="Method_Opt=>'For all columns size 1',";

	}


	# --- delete ---
	if ($opts->{d}) {
	   	if ($table) {
			print "Deleting table stats of $table\n";
			sql->run(qq(
			Begin 
			dbms_stats.delete_table_stats(
				user, $table, 
				stattab=>$stattab, statid=>$statid, 
				cascade_parts=>true, cascade_columns=>true, 
				cascade_indexes=>true); 
			end;
		));
		} else {
			print "Deleting schema stats\n";
			sql->run("Begin dbms_stats.delete_schema_stats(user); end;");
		}
	}

	# --- gather ---
	if ($opts->{g}) {
		if ($table) {
			print "Gathering table stats of $table\n";
			sql->run (qq(
			Begin
			dbms_stats.gather_table_stats(
				Ownname=>user, 
				Tabname => $table,
				stattab=>$stattab, statid=>$statid, 
				$estimatePercent
				$granularity
				$methodOpt
				cascade=>true);
			End;
		));

		} else {
			print "Gathering schema stats\n";
			sql->run (qq(
			Begin
			dbms_stats.gather_schema_stats(
				Ownname=>user, 
				stattab=>$stattab, statid=>$statid, 
				$estimatePercent
				$granularity
				$methodOpt
				cascade=>true);
			End;
		));
		}
	}
	
	# --- import ---
	if ($opts->{i}) {
		checkStattab($stattab, $statid) or return;
		if ($table) {
			print "Importing table stats of $table from $stattab/$statid\n";
			sql->run (qq(
			begin
			dbms_stats.import_table_stats(
				ownname => user,
				tabname => $table,
				stattab=>$stattab, 
				statid=>$statid, 
				statown=>user);
		        end;
		));

		} else {
			print "Importing schema stats from $stattab/$statid\n";
			sql->run (qq(
			begin
			dbms_stats.import_schema_stats(
				ownname => user,
				stattab=>$stattab, 
				statid=>$statid, 
				statown=>user);
		        end;
		));
		}
	}

	# --- export ---
	if ($opts->{e}) {
		checkStattab($stattab, $statid) or return;
		if ($table) {
			print "Exporting table stats of $table to $stattab/$statid\n";
			sql->run (qq(
			begin
			dbms_stats.export_table_stats(
				ownname => user,
				tabname => $table,
				stattab=>$stattab, 
				statid=>$statid, 
				statown=>user);
		        end;
		));
		} else {
			print "Export schema stats to $stattab/$statid\n";
			sql->run (qq(
			begin
			dbms_stats.export_schema_stats(
				ownname => user,
				stattab=>$stattab, 
				statid=>$statid, 
				statown=>user);
		        end;
		));
		}
	}

	if ($opts->{c}) {
		print "Create $stattab\n";
		sql->run("Begin dbms_stats.create_stat_table (user,$stattab); end;");
	}

	# --- print ---
	if ($opts->{p} or my $summary=$opts->{P}) {
		my $pattern;
		my $doubtClause;
		my $projection;
		my $groupOrder;
		if ($summary) {
			$pattern = $opts->{P};
			$doubtClause = "and ( last_analyzed is null or sample_size = 0)";
			$projection = "table_name, count(*) missing";
			$groupOrder = "group by table_name";
		} else {
			$pattern = $opts->{p};
			$projection = " * ";
			$groupOrder = "order by table_name";
		}
		    
		my $pattern = "'" . uc($opts->{p}.$opts->{P}) . "'";
		$pattern =~ s/\*/\%/g;
		my $doubtClause;
		$doubtClause = "and ( last_analyzed is null or sample_size = 0)" if $opts->{P};
		sql->run(qq(
		select $projection from (
			select 'Table' object_type,
			table_name table_name,
			'                          ' index_name,
			'                          ' column_name,
			num_rows,
			last_analyzed,
			sample_size
			from user_tables
		      union all
			select 'Index' object_type,
			table_name, 
			index_name index_name,
			'                          ' column_name,
			num_rows,
			last_analyzed,
			sample_size
			from user_indexes
		      union all
			select 'Table_col' object_type,
			table_name, 
			'                          ' index_name,
			column_name,
			0 num_rows,
			last_analyzed,
			sample_size
			from user_tab_columns
			where table_name in (select table_name from user_tables)
		)
		WHERE table_name like $pattern 
		$doubtClause
		$groupOrder
		));
	}
}

# ------------------------------------------------------------
$COMMANDS->{"printStats"} =
  {
	  alias => "",
	  description => "Print optimizer statistics information",
	  synopsis => 
	    [
	     ["Tablepattern"],
	     ["-e", "pattern", "exclude objects"],
	     ["-p", undef, "print partition stats too"],
	     ["-i", undef, "print index stats too"],
	     ["-m", undef, "print missing stats only"]
	     ],
	      routine => \&printStats
    };

sub printStats {
	my ($this, $argv, $opts) = @_;
	my $namePattern = "'".uc($argv->[0])."'";
	my $excludeClause = "and table_name not like " . "'".uc($opts->{e})."'" if ($opts->{e});
	my $doIndexes = $opts->{i} ? "1=1" : "1=2";
	my $doPartitions = $opts->{p} ? "1=1" : "1=2";
	my $missingOnly = "and last_analyzed is null or sample_size=0" if ($opts->{m});


	sql->run(qq(
	select * from (
		select table_name,
		'Table' object_type,
		null subobject,
		last_analyzed,
		num_rows,
		sample_size
		from user_tables
		where table_name like $namePattern $excludeClause
	      union all
		select table_name,
		'TablePartition' object_type,
		partition_name subobject,
		last_analyzed,
		num_rows,
		sample_size
		from user_tab_partitions
		where table_name like $namePattern $excludeClause
		and $doPartitions
	      union all
		select table_name,
		'Index' object_type,
		index_name subobject, 
		last_analyzed,
		num_rows,
		sample_size
		from user_indexes
		where table_name like $namePattern $excludeClause
		and $doIndexes
	      union all
		select ui.table_name,
		'IndexPartition' object_type,
		ui.index_name||'.'||uip.partition_name subobject, 
		uip.last_analyzed,
		uip.num_rows,
		uip.sample_size
		from 
			user_indexes ui,
			user_ind_partitions uip
		where ui.table_name like $namePattern $excludeClause
		and ui.index_name = uip.index_name
		and $doIndexes and $doPartitions
	)
		where 1=1
		$missingOnly
	));

}

# ------------------------------------------------------------
$COMMANDS->{"checkStats"} =
  {
	  alias => "",
	  description => "Run some plausibility checks on optimizer Statistics",
	  synopsis => 
	    [
	     [],
	     ["-p", undef, 'num_rows of partition_stats should add up to table_stats'],
	     ["-s", undef, 'if num_rows is small table/partition should really be small'],
	     ["-a", undef, 'run all tests'],
	    ],
	    routine => \&checkStats
  };

sub checkStats {
	my ($this, $argv, $opts) = @_;
	my $optionsSelected = 0;
	my $countTableRows = sub {
		my ($tableName, $rows) = @_;
		my $result = sql->runArrayref(qq(
			select count(*)
			from $tableName
		));
		return $result->[0][0];
	};

	my $countPartitionRows = sub {
		my ($tableName, $partitionName, $rows) = @_;
		my $result = sql->runArrayref(qq(
			select count(*)
			from $tableName PARTITION ($partitionName)
		));
		return $result->[0][0];
	};

	if ($opts->{p} or $opts->{a}) {
		$optionsSelected++;
		print "------------------------------------------------\n";
		print "Partition rows not adding up to Table rows:\n";
		sql->run(qq(
		select pstats.table_name, partition_rows, table_rows from
		(
			select table_name, sum(num_rows) partition_rows
			from user_tab_partitions
			group by table_name
		) pstats,
		(
			select table_name, num_rows table_rows
			from user_tables
		) tstats
		where 
			pstats.table_name = tstats.table_name
			and not (partition_rows between table_rows*0.96 and table_rows *1.04)
       	));

		sql->run(qq(
		select pstats.index_name, partition_rows, index_rows from
		(
			select index_name, sum(num_rows) partition_rows
			from user_ind_partitions
			group by index_name
		) pstats,
		(
			select index_name, num_rows index_rows
			from user_indexes
		) istats
		where 
			pstats.index_name = istats.index_name
			and not (partition_rows between index_rows*0.91 and index_rows *1.09)
       	));
	}

	if ($opts->{s} or $opts->{a}) {
		$optionsSelected++;
		print "------------------------------------------------\n";
		print "Small tables(partitions) whose size is underestimated:\n\n";
		my $result = sql->runArrayref(qq(
			select table_name, num_rows from user_tables
			where num_rows < 100
	));
		my $format = "%-32s %10d %10d\n";
		printf ("%-32s %10s %10s\n\n", 'Table(Partition)', 'Stat', 'Actual');
		foreach my $row (@$result) {
			my $table = $row->[0];
			my $rows = $row->[1];
			my $actual = $countTableRows->($table, $rows);
			printf ($format, $table, $rows, $actual) if ($actual > $rows*1.5);
		}
		

		my $result = sql->runArrayref(qq(
			select table_name, partition_name, num_rows from user_tab_partitions
			where num_rows < 100
	));
		foreach my $row (@$result) {
			my $table = $row->[0];
			my $partition = $row->[1];
			my $rows = $row->[2];
			my $actual = $countPartitionRows->($table, $partition, $rows);
			printf ($format, $table.'('.$partition.')', $rows, $actual) if ($actual > $rows*1.5);
		}
		

	}
	print "You must specify a valid option \n" unless $optionsSelected;
}
# ------------------------------------------------------------
$COMMANDS->{"analyzeObject"} =
  {
	  alias => "ana",
	  description => "analyze tables or indexes old style",
	  synopsis => 
	    [
	     ["pattern"],
	     ["-c", undef, 'compute instead of estimate'],
	     ["-t", "days", 'only if not analyzed days ago'],
	     ["-n", undef, "only show objects, don't analyze"],
	     ["-d", undef, "delete statistics"],
	     ],
	      routine => \&analyze,
    };
# ------------------------------------------------------------
sub analyze {
	my ($this, $argv, $opts) = @_;

	print "better used dbms_stats.\n";
	my ($typePattern, $namePattern) = processPattern($argv);
	my $timeClause;
	if (my $days = $opts->{t}) {
		$timeClause = qq( where last_analyzed is null
			or trunc(sysdate-last_analyzed) >= $days
		);
	}

	my $sth_objects = sql->prepare (qq(
		select * from (
			select 'INDEX', index_name, last_analyzed
			from user_indexes
			where index_name like $namePattern
			and 'INDEX' like $typePattern
			union
			select 'TABLE', table_name, last_analyzed
			from user_tables
			where table_name like $namePattern
			and 'TABLE' like $typePattern
		) $timeClause
	));
	$sth_objects->execute;
	while (my $row = $sth_objects->fetchrow_arrayref) {
		my $compute = $opts->{c} ? "compute" : "estimate";
		my $compute = $opts->{d} ? "delete" : "$compute";
		my $stmt = "analyze $row->[0] $row->[1] $compute statistics";
		if ($opts->{n}) {
			print "$stmt;\n";
		} else {
			sql->run($stmt);
		}
	}
}


# ------------------------------------------------------------
sub printWrapped {
# ------------------------------------------------------------
	my ($text, $lineLimit) = @_;
	my @lines = split (/\n/, $text);
	$lineLimit=-1 if (!defined($lineLimit));

	my $truncated=1 if $#lines+1 > $lineLimit && $lineLimit >=0;
	#print "lines $#lines\n";

	foreach my $line (@lines) {
		#print "$line\n";
		last if ($lineLimit == 0);
		$lineLimit--;
		print wrap("","\t",$line)."\n";
	}
	print "[...]\n" if $truncated;
}

# ------------------------------------------------------------
$COMMANDS->{"profile"} =
  {
	  alias => "",
	  description => "estimate current execution times",
	  synopsis => 
	    [
	     [""],
	     ["-s", "sids", 'restrict to sids'],
	     ["-c", undef, 'create required table senora_profile'],
	     ["-T", "HH:MI:SS", 'set sampling duration'],
	     ["-i", "iterations", 'set sample size (300)'],
	     ["-t", "msecs", 'set sampling time (100)'],
	     ["-n", "rows", 'show top n statement (10)'],
	     ],
   	  man => q(

	Profile is a good tool to answer the question "why is this
	taking so long ?".

	Profile runs a small plsql program, that checks v$session
	every 'msecs' milliseconds. It does this 'iteration'
	times. The finest resolution is 10 msecs.

	If the session is active, the current sql_address is stored in
	a table 'senora_profile'. Long running statements will get
	more 'hits', than fast statements and frequently executed
	statements will get more hits than rarely executed ones. Thus
	the number of hits is a good aproximation for the execution
	time of a statement.

	Finally the senora_profile table is evaluated and the
	sql_addresses are resolved into readable sql text.

	You can either set the number of iterations and the time
	between samples individually, or specify the total duration.
	In the latter case "profile" will choose longer sampling times
	for longer durations (between 100 and 10000 msec)
),

	      routine => \&profile,
    };
# ------------------------------------------------------------
sub profile {
	my ($this, $argv, $opts) = @_;

	my $sidClause = "and sid in ($opts->{s})" if $opts->{"s"};
	my $iterations = $opts->{i} ? $opts->{i} : 300;
	my $stime = $opts->{t} ? $opts->{t} : 100;
	my $etime = $stime * $iterations / 1000;
	my $toprows = $opts->{n} ? $opts->{n} : 10;

	if ($opts->{c}) {
		sql->run("drop table senora_profile");
		sql->run (qq(
			create table senora_profile 
			as select 0 sid, address,0 hits 
			from v\$sqlarea where 1=2
		));
		return;
	}

	if ($opts->{T}) {
		my ($hr,$mi,$ss) = split(":", $opts->{T});
		$etime = ($hr*60 + $mi)*60 + $ss;
		# use longer sampling intervals for longer durations
		if ($etime < 600) {$stime = 100;}
		elsif ($etime < 6000) {$stime = 1000;}
		else {$stime = 10000;}
		$iterations = $etime*1000/$stime;
	}

	printf("$iterations iterations every $stime msecs\n");
	printf("Estimated sampling time = %02d:%02d:%02d (%d seconds)\n", 
	       int($etime/3600), (gmtime($etime))[1,0], $etime);

	sql->run("delete from senora_profile");
	my $sth = sql->prepare(qq(
	declare
		type sql_address_t is table of varchar2(64) 
			index by binary_integer;
		sql_address sql_address_t;
		type sid_t is table of number index by binary_integer;
		vsid sid_t;
		j number := 1;
	begin
		for i in 1..$iterations
		loop
			for s in (
				select sid, rawtohex(sql_address) sql_address
				from gv\$session 
				where status='ACTIVE' $sidClause
			) loop
				vsid(j) := s.sid;
				sql_address(j) := s.sql_address;
				j := j+1;
			end loop;
			dbms_lock.sleep($stime/1000);
		end loop;
		delete from senora_profile;
		for i in 1..j-1
		loop
			update senora_profile
			set hits = hits + 1
			where address = sql_address(i)
			;
			if SQL%ROWCOUNT=0 then
				insert into senora_profile
				(sid, address, hits)
				values (vsid(i),hextoraw(sql_address(i)), 1)
				;
			end if;
		end loop;
	end;
	));
	Feedback::printError ($this->{sth});
	return unless $sth;
	$sth->execute;
	Feedback::printError ($this->{sth});

	my $sth=sql->prepare (qq(
		select b.sid, a.address, hits, sql_text current_statement
		from 
			gv\$sqltext_with_newlines a,
			senora_profile b
		where a.address = b.address
		order by hits desc, a.address, piece
	));
	$sth->execute or return;
	my $last_address='undef';
	my $statement=undef;
	my $rows=0;
	while (my $row = $sth->fetchrow_arrayref) {
		$row->[3]=~s/·//;
		if ($row->[0].$row->[1] ne $last_address) {
			$rows++;
			printWrapped($statement,20);
			$statement=undef;
			print "\n------------------------\n";
			printf "($rows) Hits=$row->[2]/$iterations Sid=$row->[0] ($row->[1])\n";
			print "------------------------\n";
			$last_address=$row->[0].$row->[1];		}
		$statement .= $row->[3];
		last if $rows >= $toprows
	}
	printWrapped($statement,20);
	print "\n";

	# make sure all locks are gone
	sql->run("commit");

}

# ------------------------------------------------------------
$COMMANDS->{"FkeyIndexes"} =
  {
	  alias => "fki",
	  description => "analyze the existance of foreign key indexes",
	  synopsis => 
	    [
	     [""],
	     ["-i", undef, 'show only missing indexes'],
	     ["-s", undef, 'create script'],
	     ["-S", "subst", 'subsitution for index name e.g. -Ss/_FK_/_IT_/'],
	     ["-I", "initial", 'storage Clause'],
	     ],
   	  man => q(
   	Provide a listing of foreign keys, their columns and the
   	indexes used on these columns.

	You can create a script that creates missing indexes.
	The names of the indexes are constructed from the
	names of the constraints (-S option).

),

	      routine => \&FkeyIndexes,
    };
sub FkeyIndexes {
	my ($this, $argv, $opts) = @_;
	my $missingClause = "and b.table_name is null" if $opts->{i};

	my $script = qq<
	select decode( b.table_name, NULL, 'missing', 'ok' ) Status, 
		   a.table_name, a.constraint_name, a.columns, b.columns icolumns
	from 
	( select 
		substr(a.table_name,1,30) table_name, 
	     	substr(a.constraint_name,1,30) constraint_name, 
		 max(decode(position, 1,     substr(column_name,1,30),NULL)) || 
		 max(decode(position, 2,', '||substr(column_name,1,30),NULL)) || 
		 max(decode(position, 3,', '||substr(column_name,1,30),NULL)) || 
		 max(decode(position, 4,', '||substr(column_name,1,30),NULL)) || 
		 max(decode(position, 5,', '||substr(column_name,1,30),NULL)) || 
		 max(decode(position, 6,', '||substr(column_name,1,30),NULL)) || 
		 max(decode(position, 7,', '||substr(column_name,1,30),NULL)) || 
		 max(decode(position, 8,', '||substr(column_name,1,30),NULL)) || 
		 max(decode(position, 9,', '||substr(column_name,1,30),NULL)) || 
		 max(decode(position,10,', '||substr(column_name,1,30),NULL)) || 
		 max(decode(position,11,', '||substr(column_name,1,30),NULL)) || 
		 max(decode(position,12,', '||substr(column_name,1,30),NULL)) || 
		 max(decode(position,13,', '||substr(column_name,1,30),NULL)) || 
		 max(decode(position,14,', '||substr(column_name,1,30),NULL)) || 
		 max(decode(position,15,', '||substr(column_name,1,30),NULL)) || 
		 max(decode(position,16,', '||substr(column_name,1,30),NULL)) 
		columns
	    from user_cons_columns a, user_constraints b
	   where a.constraint_name = b.constraint_name
	     and b.constraint_type = 'R'
	   group by substr(a.table_name,1,30), substr(a.constraint_name,1,30) ) a, 
	( select substr(table_name,1,30) table_name, substr(index_name,1,30) index_name, 
		     max(decode(column_position, 1,     substr(column_name,1,30),NULL)) || 
		     max(decode(column_position, 2,', '||substr(column_name,1,30),NULL)) || 
		     max(decode(column_position, 3,', '||substr(column_name,1,30),NULL)) || 
		     max(decode(column_position, 4,', '||substr(column_name,1,30),NULL)) || 
		     max(decode(column_position, 5,', '||substr(column_name,1,30),NULL)) || 
		     max(decode(column_position, 6,', '||substr(column_name,1,30),NULL)) || 
		     max(decode(column_position, 7,', '||substr(column_name,1,30),NULL)) || 
		     max(decode(column_position, 8,', '||substr(column_name,1,30),NULL)) || 
		     max(decode(column_position, 9,', '||substr(column_name,1,30),NULL)) || 
		     max(decode(column_position,10,', '||substr(column_name,1,30),NULL)) || 
		     max(decode(column_position,11,', '||substr(column_name,1,30),NULL)) || 
		     max(decode(column_position,12,', '||substr(column_name,1,30),NULL)) || 
		     max(decode(column_position,13,', '||substr(column_name,1,30),NULL)) || 
		     max(decode(column_position,14,', '||substr(column_name,1,30),NULL)) || 
		     max(decode(column_position,15,', '||substr(column_name,1,30),NULL)) || 
		     max(decode(column_position,16,', '||substr(column_name,1,30),NULL)) columns
	    from user_ind_columns 
	   group by substr(table_name,1,30), substr(index_name,1,30) ) b
	where a.table_name = b.table_name (+)
	  and b.columns (+) like a.columns || '%' 
	$missingClause
	>;;
	if ($opts->{"s"}) {
		my $storage = qq (\nStorage (INITIAL $opts->{I} NEXT $opts->{I})) if $opts->{"I"};
		my $sth = sql->prepare($script);
		$sth->execute;
		while (my $row=$sth->fetchrow_arrayref) {
			my $indexName = $row->[2];
			my $subst = "\$indexName =~ $opts->{S}";
			#print "$subst\n";
			eval "$subst";
			print ("Create index $indexName\n"
			  ."on $row->[1] ($row->[3])$storage;\n\n");
		}
	} else {
		sql->run($script);
	}
}


# ------------------------------------------------------------
$COMMANDS->{"cacheStats"} =
  {
   alias => "cst",
   description => "Display memory statisticts",
   synopsis => 
	    [
	     [""],
	     ],
   routine => \&cacheStats
  };

sub cacheStats {
	my ($this, $argv, $opts) = @_;

	print ("** dictionary cache hit ratio");
	sql->run(q(
		select 	sum(gets) "Gets", sum(getmisses) "Misses",
			trunc((1 - (sum(getmisses) / (sum(gets) +     	
			sum(getmisses))))*100) "HitRate"
		from  	v$rowcache
	));
	print ("\n** library cache hit ratio ");
	sql->run(q(
		select 	sum(pins) Executions, sum(pinhits) "Execution Hits", 
			trunc(((sum(pinhits) / sum(pins)) * 100)) phitrat,
			sum(reloads) Misses,
			trunc(((sum(pins) / (sum(pins) + sum(reloads))) * 100))  hitrat
		from	v$librarycache
	));

	print ("\n** shared pool");
	sql->run(q(
		select	to_number(v$parameter.value) value, v$sgastat.bytes, 
			trunc((v$sgastat.bytes/v$parameter.value)*100) "Percent Free"
		from	v$sgastat, v$parameter
		where	v$sgastat.name = 'free memory'
		and	v$parameter .name = 'shared_pool_size'
	));
	sql->run(q(
		select	sum(ksmchsiz) Bytes, ksmchcls Status
		from 	x$ksmsp
		group	by ksmchcls
	));

	print ("\n** DB block buffer usage");
	sql->run(q(
		select 	decode(state,0, 'FREE', 1, decode(lrba_seq,0,'AVAILABLE','BEING USED'), 
			3, 'BEING USED', state) "BLOCK STATUS", count(*)
		from 	x$bh
		group 	by decode(state,0,'FREE',1,decode(lrba_seq,0,
			'AVAILABLE','BEING USED'),3, 'BEING USED', state)
	));

}




# --------------------------------------------------
$COMMANDS->{"bufferCacheHitRatio"} =
  {
   alias => "bchr",
   description => "show the buffer cache hit ratio",
   synopsis => 
	    [
	     [""],
	     ],
   routine => \&bufferCacheHitRatio
  };

sub bufferCacheHitRatio {
	my ($this, $argv, $opts) = @_;
	sql->run (qq(
		  select
		  100 - trunc(100*(physical.value/(blockgets.value + consistent.value)),2) hit_ratio,
		  physical.value physical_reads,
		  blockgets.value block_gets,
		  consistent.value consistent_gets
		  from 
		  v\$sysstat physical,
		  v\$sysstat blockgets,
		  v\$sysstat consistent
		  where
		  1=1
		  and physical.name = 'physical reads'
		  and blockgets.name = 'db block gets'
		  and consistent.name = 'consistent gets'
		  ));

}

# --------------------------------------------------
$COMMANDS->{"openCursors"} =
  {
   alias => "opc",
   description => "show open cursors",
   synopsis => 
	    [
	     [""],
	     ["-l", undef, "show statements too"],
	     ["-n", "low", "show only if cursors > low"],
	     ["-s", "sid", "only show this sid"]
	     ],
   man => q(
	Without -l values are taken from v$sesstat, which is said to
	be more correct than v$open_cursor. With -l v$open_cursor is
	used, so there might be a mismatch,
),
   routine => \&openCursors
  };

sub openCursors {
	my ($this, $argv, $opts) = @_;
	my $limit = $opts->{n} ? $opts->{n} : 0;
	my $sidClause = "";
	$sidClause = "and c.sid = " . $opts->{s} if $opts->{s};

	if ($opts->{l}) {
		sql->run (qq( 
		select c.sid, s.username, s.osuser, s.program,  count(*) cursors , sql_text
		from v\$open_cursor c,
		v\$session s
		where c.sid = s.sid $sidClause
		group by c.sid, s.username, s.osuser, sql_text, s.program
		having count(*) > $limit
		order by cursors desc
		));

	} else {
		sql->run (qq( 
		select 
			a.sid, 
			c.username,
			c.osuser,
			c.program,
			a.value cursors
		from 
			v\$sesstat a, 
			v\$statname b, 
			v\$session c
		where 
			a.statistic# = b.statistic#
		and 	b.name = 'opened cursors current'
		and 	a.sid = c.sid
		$sidClause
		and	a.value > $limit
		order by cursors desc
		));
	}
}
# --------------------------------------------------
$COMMANDS->{"alertLog"} =
  {
   alias => undef,
   description => "get data from alert_log",
   synopsis => 
	    [
	     [""],
	     ["-c", undef, "initialize (as sys)"],
	     ["-C", undef, "cleanup"],
	     ["-p", "pattern", "grep lines matching pattern"],
	     ["-p", "pattern", "specify pattern not to match"],
	     ["-t", "pattern", "grep lines matching time"],
	     ["-V", "version", "set oracle version to 8 or 9 (default=8)"],
	     ],
   man => q(

	Make the alert log accessible as table and provide some
	shorthand methods for reading the alert log. After
	initializing (-c) you can also access the alert log with plain
	SQL.

	The Oracle 8 version requires utl_file_dir to allow access to
	bdump and can be run as a normal user. You must rerun the
	initialization every time you want to see fresh data in the
	alert log.

	The Oracle 9 version uses an external table and must be run as
	sys. The Oracle 8 version can be used uner oracle 9 too.

EXAMPLES
	alertLog -p starting -t Oct.*2004
		shows all "starting" events in Oct 2004

	alertLog -p advanced.to -t Oct.*2004
		shows log switches in Oct 2004
TABLES
	SENORA_BDUMP 		directory object (9i only)
	SENORA_ALERT_LOG	table representing the alert log
BUGS
	Patterns must be regular expressions without spaces, not Oracle wildcards
),

   routine => \&alertLog
  };

sub alertLog {
	my ($this, $argv, $opts) = @_;
	my $bgDumpDest;
	my $alertLog;
	my $dir = "SENORA_BDUMP";
	my $table = "SENORA_ALERT_LOG";
	if ($opts->{c}) {
		# get background_dump_dest
		my $sth=sql->prepare("select value from v\$parameter where name = 'background_dump_dest'");
		$sth->execute;
		$bgDumpDest=$sth->fetchrow_arrayref()->[0];

		# get name of alert log
		my $sth=sql->prepare("select instance_name from v\$instance");
		$sth->execute;
		my $instanceName=$sth->fetchrow_arrayref()->[0];
		$alertLog="alert_$instanceName.log";

		# setup for oracle 9
		if ($opts->{V} eq 9) {
			# create directory and table
			sql->run(qq(CREATE OR REPLACE DIRECTORY $dir AS '$bgDumpDest'));
			print "Created directory $dir -> $bgDumpDest\n";
			sql->run(qq(grant read,write on directory $dir to select_catalog_role));
			print "granted access to $dir to SELECT_CATALOG_ROLE\n";
			sql->run("drop table $table");
			sql->run(qq(
			CREATE TABLE sys.$table (text VARCHAR2(400))
			ORGANIZATION EXTERNAL (
			TYPE oracle_loader
			DEFAULT DIRECTORY $dir
			ACCESS PARAMETERS (
			  RECORDS DELIMITED BY newline
			  nobadfile
			  nodiscardfile
			  nologfile
			  NOBADFILE NODISCARDFILE NOLOGFILE)
			  location('$alertLog'))
			REJECT LIMIT unlimited
		));
			sql->run("grant all on sys.alert_log to public");
			print ("Table $table ($alertLog) granted to public\n\n");
		} else {
			# setup for oracle 8
			sql->run("drop table $table");
			sql->run("create table $table (text varchar2(500))");
			sql->run(qq(
		declare
			fd utl_file.file_type;
			buffer varchar2(500);
		begin
		fd := utl_file.fopen('$bgDumpDest','$alertLog','r');
		  IF utl_file.is_open(fd) THEN
		      LOOP
			 BEGIN
			    utl_file.get_line(fd, buffer);

			    IF buffer IS NULL THEN
			       NULL; --EXIT;
			    END IF;

			    INSERT INTO $table (text)
			    VALUES (buffer);
			 EXCEPTION
			    WHEN NO_DATA_FOUND THEN 
			       dbms_output.put_line('Finished reading alert log');
				utl_file.fclose(fd);
				commit;
			       return;
			 END;
		      END LOOP;
		      COMMIT;
		   END IF;
		utl_file.fclose(fd);
		end;));
		}
		return;
	}
	if ($opts->{C}) {
		sql->run("drop table senora_alert_log");
		return;
	}
	return unless ($opts->{p} || $opts->{T});
	my $pattern = $opts->{p} ? $opts->{p} : ".";
	my $antiPattern = $opts->{P};
	my $timePattern=$opts->{t} ? $opts->{t} : ".";

	my $sth = sql->prepare("select * from $table");
	$sth->execute;
	my $date;
	my $lastRow;
	my $conecutiveRows=0;
	my $dateOk = 1;
	while (my $r = $sth->fetchrow_arrayref) {
		my $row = $r->[0];
		if ($row =~ /... ... .. ..:..:.. ..../) {
			$date = $row;
		}
		if ($row =~ /$pattern/i && $row !~ /$antiPattern/) {
			if ($date =~ /$timePattern/) {$dateOk=1;} else {$dateOk=0;}
			if ($row eq $lastRow) {
				$conecutiveRows++;
			}
			if ($dateOk) {
				if ($conecutiveRows) {
					print "\t*** last message repeated $conecutiveRows times ***\n";
					$conecutiveRows=0;
				} else {
					print "$date\n";
					print "\t$row\n";
					$lastRow=$row;
				}
			}
		}
	}
}



# --------------------------------------------------
$COMMANDS->{"benchmark"} =
  {
   alias => undef,
   description => "Estimate machine performance",
   synopsis => 
	    [
	     [""],
	     ],
   routine => \&benchmark
};

sub benchmark {
	my ($this, $argv, $opts) = @_;

	Bind::declareVar(":bench", 80, ""); sql->run(qq( declare
	startTime number; endTime number; elapsed number; rating
	number; foo number; bar varchar2(1024) := ''; begin select
	TO_NUMBER(TO_CHAR(systimestamp, 'SSSSSFF3')) into startTime
	from dual;

			for i in 1..1000
			loop
				bar := '';
				for j in 1..100
				loop
					select sqrt(j)
					into foo
					from dual
					;
					bar := bar || j;
					null;
				end loop;
			end loop;

			select TO_NUMBER(TO_CHAR(systimestamp, 'SSSSSFF3')) 
			into endTime
			from dual;

			elapsed := endTime - startTime;
			rating := trunc(10000/elapsed,1);
			dbms_output.put_line('Elapsed = ' || elapsed || ', Rating = ' || rating);
			:bench := ('Elapsed = ' || elapsed || ', Rating = ' || rating);
		end;
	), "plsql");
	Bind::printVar(":bench");
}


# --------------------------------------------------
$COMMANDS->{"fetchtest"} =
  {
   alias => undef,
   description => "Estimate fetch performance",
   synopsis => 
	    [
	     [""],
	     ],
   routine => \&fetchtest
};

sub fetchtest {
	my ($this, $argv, $opts) = @_;

	my ($sec, $usec) = Time::HiRes::gettimeofday();
	my $msStart = int(($sec*1000 + $usec/1000) + 0.5);


	my $stmt = "@$argv";
	print "$stmt\n";
	my $result = sql->runArrayref($stmt);

	my ($sec, $usec) = Time::HiRes::gettimeofday();
	my $msEnd = int(($sec*1000 + $usec/1000) + 0.5);

	printf ("Elapsed: %d msec\n", $msEnd - $msStart);
	
}

1;	
