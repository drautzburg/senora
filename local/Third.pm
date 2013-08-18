# --------------------------------------------------------------------------------  
# NAME
#   Thirds - Funktions for the third system
# RESPONSIBILITIES
#
# COLLABORATIONS
#
# History
# --------------------------------------------------------------------------------  
# $log$
# --------------------------------------------------------------------------------  

=head1 NAME

Third - Functions for the third system

=head1 RESPONSIBILITIES

=head1 COLLABORATIONS

=cut


package Third;
use FileHandle;
use Misc;
use Settings;
use strict;
use Plugin;
use SessionMgr qw(sessionNr sessionMain sql);
use XML::Simple;
use XML::CsfSortplan;
my $line = "==========================================================================================\n";

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

# ============================================================
# profiler
# ============================================================

my $timings = {};
my $fromTime = undef;
my $toTime = undef;


$COMMANDS->{"tProfile"} =
  {
	  alias => "tprof",
	  description => "Analyze trace_log",
	  synopsis => 
	    [
	     [],
	     ["-t", "hours", "go back this many hours(1)"],
	     ["-C", undef, "sort by msec/call instead of elapsed"],
	     ["-T", "table", "user table instead of trace_log"],
	     ["-p", "pattern", "find longest execution times for pattern"],
	     ],
	      routine => \&tProfile
    };


sub tProfile {
	my ($this, $argv, $opts) = @_;
	$timings={};
	$fromTime=undef;
	$toTime=undef;
	my $i=0;
	$|++; # no buffering

	my $table = "TRACE_LOG";
	$table = $opts->{T} if $opts->{T};

	my $hours = $opts->{t} ? $opts->{t} : 1;
	$hours = "$hours/24";


	# find longest execution times (hotspots)
	if (my $p = $opts->{p}) {
	$p="'%".$p."%'";
	sql->run (qq(
		select *
		  from (SELECT substr(next_timestamp - trc_timestamp,
				      11) elapsed,
			       sid||','||trc_message in_trace,
			       substr(next_message,
				      6 + instr(next_message,
						'<out>')) out_trace
			  FROM (SELECT LEAD(trc_timestamp) OVER(PARTITION BY trc_session ORDER BY trc_id, substr(trc_message, 1, instr(trc_message, '<out>'))) next_timestamp,
				       LEAD(trc_message) OVER(PARTITION BY trc_session ORDER BY trc_id, substr(trc_message, 1, instr(trc_message, '<out>'))) next_message,
				       trc_id,
				       trc_timestamp,
				       substr (trc_session,1,3) sid,
				       trc_message
				  FROM (SELECT trc_id, trc_session, trc_timestamp, trc_message
					  FROM $table
					 WHERE trc_timestamp > sysdate - $hours
					    AND (trc_message LIKE $p || '%<in>%'
					    OR  trc_message LIKE $p || '%<out>%')))
			 WHERE trc_message LIKE $p || '%<in>%'
			 ORDER BY 1 desc)
		 where rownum < 25
	));
	} else {

		my $sth = sql->prepare (qq(
		select to_char(trc_timestamp,'DD.MM.YY HH24:MI:SS,FF3') trc_timestamp, trc_message, trc_session
		from $table where trc_timestamp > sysdate - $hours
		order by trc_id
				       ));
		print "Sorting ...\n";
		$sth->execute;

		my $dateRegexp = "[0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]";
		my $timeRegexp = "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]\,[0-9][0-9][0-9]";
		my $methodRegexp = '[A-Z][A-Z][A-Z] \<[ \.\w]*\>';
		my $inOutRegexp = '<in>|<out>|<dbg>';
		my $sidRegexp	= '[0-9]+,';

		while (my $row = $sth->fetchrow_arrayref) {
			$i++;

			# get the intersting fiels of each row
			#print "[0] $row->[0]\n";
			$row->[0] =~ /($dateRegexp) ($timeRegexp)/;
			my $date = $1;
			my $time = $2;

			#print "[1] $row->[1]\n";
			$row->[1] =~ /($methodRegexp) ($inOutRegexp)/;
			my $method = $1;
			my $inout = $2;

			#print "[2] $row->[2]\n";
			$row->[2] =~ /($sidRegexp|AMB.*).*/;
			my $sid=$1;
			if ($sid =~ /AMB.*/) { $sid='AMB,';}
			$method = $sid.$method;

			#print "\ndate $date time $time meth $method; io $inout " . asMilliseconds($time) . "\n";

			if  ($i%20000 == 0) {
				print "$i rows processed \n";
			}

			if ($inout eq "<in>") {
				startOfMethod($method, $time);
			}
			elsif ($inout eq "<out>") {
				endOfMethod($method, $time);
			}
			else {
				# there are some multiline trace entries. These will lead here
				# print "In or out ? $line";
				next;
			}

			# we only get here if the row was good.
			adjustTimeRange($date, $time);

		}
		print "$i rows processed \n\n";
		printReportTime();

		my $sortOrder = $opts->{C} ? 'perCall': 'elapsed';
		printTimings(40, $sortOrder);
		print "\n";
		printMisses();
	}
}

# ----------------------------------------
sub adjustTimeRange {
	my ($date, $time) = @_;
	my $timeString = "$date $time";
	# print "from $fromTime to $toTime string $timeString\n";
	if (!$fromTime || $fromTime gt $timeString) {
		$fromTime = $timeString;
	}
	if (!$toTime || $toTime lt $timeString) {
		$toTime = $timeString;
	}

}
# ----------------------------------------
sub asMilliseconds {
	# convert time string (eg "18:18:12,781") into milliseconds

	my ($time) = @_;
	#print "time=$time\n";
	$time =~ /([0-9][0-9]):([0-9][0-9]):([0-9][0-9]),([0-9][0-9][0-9])/;
	my ($hr, $min, $sec, $msec) = ($1,$2,$3,$4);
	my $msecs = $hr*60*60*1000 + $min*60*1000 + $sec*1000 + $msec;
	#print "hhh $hr, $min $sec $msec $msecs\n";
	return $msecs;
}

# ----------------------------------------
sub startOfMethod {
	# remember the start time of a method. Start time should not be set.

	my ($method, $time) = @_;
	push (@{$timings->{$method}->{start}}, asMilliseconds($time));
}

sub nvl0 { my $a = shift;  return (($a == undef) ? 0 : $a);}
sub nvlx { my $a = shift;  return (($a == undef) ? 9999999999 : $a);}
sub maxv { my $a = nvl0(shift); my $b = nvl0(shift); return $a > $b ? $a : $b;}
sub minv { my $a = nvlx(shift); my $b = nvlx(shift); return $a <= $b ? $a : $b;
}
# ----------------------------------------
sub endOfMethod {
	# Compute the elapsed time and increment "calls". Start time should be set by now.
	# This method cannot handle recrsive calls

	my ($method, $time) = @_;
	my $startTime = pop (@{$timings->{$method}->{start}});

	if (defined ($startTime)) {
	   	my $elapsed = asMilliseconds($time) - $startTime;
		$timings->{$method}->{elapsed} += $elapsed;
		$timings->{$method}->{calls}++;
		$timings->{$method}->{min} = minv($timings->{$method}->{min}, $elapsed);
		$timings->{$method}->{max} = maxv($timings->{$method}->{max}, $elapsed);
	} else {
		$timings->{$method}->{missingEntries}++;
		#print "Missing entry for $method\n";		
	}
}

# ----------------------------------------
sub compareMsecsPerCall {
	# helper for sorting. Compare the timings of two methods

	my ($meth1, $meth2) = @_;
	my $t1 = $timings->{$meth1}->{calls} ? 
	  $timings->{$meth1}->{elapsed}/$timings->{$meth1}->{calls} 
	    : -1;
	my $t2 = $timings->{$meth2}->{calls} ? 
	  $timings->{$meth2}->{elapsed}/$timings->{$meth2}->{calls} 
	    : -1;
	return $t1 <=> $t2;
}

# ----------------------------------------
sub compareElapsed {
	# helper for sorting. Compare the timings of two methods

	my ($meth1, $meth2) = @_;
	my $t1 = $timings->{$meth1}->{elapsed};
	my $t2 = $timings->{$meth2}->{elapsed};
	return $t1 <=> $t2;
}

# ----------------------------------------
sub hline {
	print "----------------------------------------------------------------------------------------------------\n";
}

# ----------------------------------------
sub printReportTime {
	# print the time scope of this report. Strip seconds and milliseconds
	my $ft = $fromTime;
	my $tt = $toTime;
	$ft =~ s/:[0-9][0-9]\.[0-9][0-9][0-9]//;
	$tt =~ s/:[0-9][0-9]\.[0-9][0-9][0-9]//;
	my $duration = asMilliseconds($toTime) - asMilliseconds($fromTime);
	my $seconds = int($duration/1000);
	my $minutes = int($seconds/60);
	$seconds = $seconds - 60*$minutes;
	my $hours = int($minutes/60);
	$minutes = $minutes - 60*$hours;
	print "Report time = $ft - $tt. Duration = $hours:$minutes:$seconds ($duration msecs)\n";
}
# ----------------------------------------
sub printTimings {
	my ($nrows, $sortOrder) = @_;
	my @keys = keys(%$timings);

	my @sortedKeys;
	if ($sortOrder eq 'elapsed') {
		@sortedKeys = sort {compareElapsed($b, $a)} @keys;
	} elsif ($sortOrder eq 'perCall') {
		@sortedKeys = sort {compareMsecsPerCall($b, $a)} @keys;
	}

	hline();
	printf ("%-55s %8s %6s %8s %8s %8s\n", "sid,method", "elapsed", "calls", "msec/call", "min", "max");
	hline();
	foreach my $key (@sortedKeys) {
		my $timing = $timings->{$key};
		my $misses = undef;
		printf ("%-55s %8d %6d %8d %8d %8d\n",
			$key, 
			$timing->{elapsed}, 
			$timing->{calls},
			$timing->{calls} ? $timing->{elapsed}/$timing->{calls} : -1,
			$timing->{min},
			$timing->{max}
		       );
		if ($nrows-- == -1) {
			return;
		}
	}
}
# ----------------------------------------
sub printMisses {
	my ($rows) = @_;
	my @keys = keys(%$timings);
	hline();
	foreach my $key (@keys) {
		my $entries = 0 + $timings->{$key}->{missingEntries};
		my $exits = 0 + $timings->{$key}->{missingExits};
		if ($entries + $exits) {
			printf ("%-55s %4d entries and %4d exits missing\n",
			$key, $entries, $exits);
		}
	}
}

# ============================================================
# profiler end
# ============================================================

# ------------------------------------------------------------
$COMMANDS->{"find_ocos"} =
  {
	  alias => "foco",
	  description => "Show outline contracts",
	  synopsis => 
	    [
	     [],
	     ["-i", "oco_id", ''],
	     ],
	      routine => \&outlineContracts,
    };

sub outlineContracts {
	my ($this, $argv, $opts) = @_;

	my $statement = qq(
	select
	  sloc.loc_id, sloc.name sloc, sdom.dom_id, sdom.name sdom, 
	  rloc.loc_id, rloc.name rloc, rdom.dom_id, rdom.name rdom
	from
	  outline_contract oco,
	  location sloc,
	  domain sdom,
	  location rloc,
	  domain rdom
	where
	  oco.par_id = 1
	  and oco.oco_id = $opts->{i}
	  and oco.par_id = sdom.par_id
	  and oco.dom_id_sender = sdom.dom_id
	  and oco.par_id = rdom.par_id
	  and oco.dom_id_recipient = rdom.dom_id
	  and sdom.par_id = sloc.par_id
	  and sdom.loc_id = sloc.loc_id
	  and rdom.par_id = rloc.par_id
	  and rdom.loc_id = rloc.loc_id
	);
	sql->run($statement);
}


# ------------------------------------------------------------
$COMMANDS->{"find_domain"} =
  {
   alias => 'fdom',
   description => "find domains by name or ID",
   synopsis =>
   [
    [],
    ["-L", "locPattern" , "show all domains of Location"],
    ["-d", "[par_id,]dom_id", "show domain by ID"],
    ["-l", "[par_id,]dom_id", "show domain by loc_id"]
   ],
   routine => \&tdomain
};

sub optionalParId {
	my ($string) = @_;

	my ($a, $b) = split (",", $string);
	# par_id is optional
	if (defined $b) {
		return ($a,$b);
	} else {
		return (1,$a);
	}
}

sub tdomain {
	my ($this, $argv, $opts) = @_;

	my $restriction;

	if (my $locName=$opts->{L}) {
		$restriction = qq(and upper(l.name) like '%'||upper('$locName')||'%');
	}

	if (defined (my $ids = $opts->{d})) {
		my ($par_id, $dom_id) = optionalParId ($opts->{d});
		$restriction = qq (and d.par_id = $par_id and d.dom_id = $dom_id);
	}

	if (defined (my $ids = $opts->{l})) {
		my ($par_id, $loc_id) = optionalParId ($opts->{l});
		$restriction = qq (and d.par_id = $par_id and d.loc_id = $loc_id);
	}

	my $stmt = qq(
	    select d.par_id, d.loc_id, dom_id, l.name location, d.name domain
	    from 
	      location l,
	      domain d
	    where l.par_id = d.par_id
	    and   l.loc_id = d.loc_id
	    $restriction
	      );
	sql->run($stmt);
}

# ------------------------------------------------------------
$COMMANDS->{"find_contracts"} =
  {
	  alias => 'fcon',
	  description => "Show contracts",
	  synopsis => 
	    [
	     [],
	     ["-a", undef, 'show all, not just sorting domains'],
	     ["-d", "pattern", 'restrict sender domain'],
	     ["-D", "pattern", 'restrict receiver domain'],
	     ["-l", "pattern", 'pattern for sender location'],
	     ["-L", "pattern", 'pattern for receiver location'],
	     ["-p", "par_id", 'Release'],
	     ],
	      routine => \&contracts,
    };

sub contracts {
	my ($this, $argv, $opts) = @_;

	my $typeClause = qq(
	  	and sdom.dom_type = 'S'
	    	and rdom.dom_type = 'S') unless $opts->{"a"};
	my $nameClause;
	$nameClause .= qq(
		and upper(sloc.name) like upper('%$opts->{"l"}%')
	) if $opts->{"l"};
	$nameClause .= qq(
		and upper(rloc.name) like upper('%$opts->{"L"}%')
	) if $opts->{"L"};
	$nameClause .= qq(
		and upper(sdom.name) like upper('%$opts->{"d"}%')
	) if $opts->{"d"};
	$nameClause .= qq(
		and upper(rdom.name) like upper('%$opts->{"D"}%')
	) if $opts->{"D"};
	my $parClause = "and     rloc.par_id = ".$opts->{p} if ($opts->{p});


	my $statement = qq(
		select
			oco.par_id,
			oco.oco_id,
			con.con_id,
			cop.sop_id,
			sloc.name sender_loc,
			sdom.name sender_dom,
			ssse.name sender_sse,
			rloc.name recipient_loc,
			rdom.name recipient_dom,
			rsse.name recipient_sse,
			con.name contract,
			cop.name contract_product
		from
			contract_product_v cop,
			contract_v con,
			outline_contract_v oco,
			domain_v rdom,
			location_v rloc,
			domain_v sdom,
			location_v sloc,
			hom,
			him,
			sse ssse,
			sse rsse

		where 1=1
		$parClause
		and	rloc.par_id = rdom.par_id
		and	rdom.par_id = oco.par_id
		and	oco.par_id = con.par_id
		and	con.par_id = cop.par_id
		and	oco.par_id = sdom.par_id
		and	sdom.par_id = sloc.par_id
		----------------------------------------
		and	rloc.loc_id = rdom.loc_id
		and	rdom.dom_id = oco.dom_id_recipient
		and	oco.oco_id = con.oco_id	
		and	con.con_id = cop.con_id
		and	oco.dom_id_sender = sdom.dom_id
		and	sdom.loc_id = sloc.loc_id
		----------------------------------------
		and     hom.par_id = con.par_id
		and     hom.con_id = con.con_id
		and     hom.par_id = ssse.par_id
		and     hom.sse_id = ssse.sse_id
		and     him.par_id = con.par_id
		and	him.con_id = con.con_id
		and	him.par_id = rsse.par_id
		and	him.sse_id = rsse.sse_id
		----------------------------------------
		$nameClause $typeClause
		and	rownum < 1000
		order by 1,2,3,4,5,6
	);

	sql->run($statement);

}

# ------------------------------------------------------------
$COMMANDS->{"find_cops"} =
  {
	  alias => 'fcop',
	  description => "Show where contract_products are used",
	  synopsis => 
	    [
	     [],
	     ["-l", "pattern", 'pattern for sender location (%)'],
	     ["-L", "pattern", 'pattern for receiver location (%)'],
	     ["-s", "pattern", 'pattern for sortplan (%)'],
	     ["-c", "pattern", 'pattern for contract_product (%)'],
	     ["-p", "par_id", 'Release (1)'],
	     ],
	      routine => \&find_cops,
    };


sub find_cops {
	my ($this, $argv, $opts) = @_;
	
	# a closure:
	my $nameClause = sub {
		my ($option, $column) = @_;
		my $o = $opts->{$option};
		return '' unless $o;
		$o = "'$o%'"; 
		return "and upper($column) like upper($o)";
	};

	my $valueClause = sub {
		my ($option, $column, $default) = @_;
		my $o = $opts->{$option};
		if ($o) {
			return "and $column = $o";		
		} else {
		  	return "and $column = $default";
		}
	};
	
	my $statement = qq(
		select
			sloc.name sender_loc,
			sdom.name sender_dom,
			rloc.name recipient_loc,
			rdom.name recipient_dom,
			oco.par_id,
			sse.name scheme,
			spl.name sortplan,
			ogr.name outlet_group,
			cop.name contract_product,
			con.name contract	
		from
			outline_contract oco,
			domain rdom,
			domain sdom,
			location rloc,
			location sloc,
			contract con,
			contract_product_v cop,
			local_transport lot,
			sortout sot,
			outlet_group ogr,
			sortplan_v spl,
			has_sortplan hsp,
			sorting_scheme sse
		where
			1=1
		and	oco.par_id = rdom.par_id
		and	oco.dom_id_recipient = rdom.dom_id
		and	oco.dom_id_sender = sdom.dom_id
		and	rdom.par_id = rloc.par_id
		and	rdom.loc_id = rloc.loc_id
		and	sdom.par_id = sloc.par_id
		and	sdom.loc_id = sloc.loc_id
		and	oco.par_id = con.par_id
		and	oco.oco_id = con.oco_id
		and	con.par_id = cop.par_id
		and	con.con_id = cop.con_id
		and	cop.par_id = lot.par_id
		and	cop.sop_id = lot.sop_id_recipient
		and	cop.sop_type = lot.sop_type_recipient
		and	lot.par_id = sot.par_id
		and	lot.sop_id = sot.sop_id
		and	lot.sop_type = sot.sop_type
		and	sot.par_id = spl.par_id
		and	sot.sop_id_sortplan = spl.sop_id
		and	sot.sop_type_sortplan = spl.sop_type
		and	sot.par_id = ogr.par_id(+)
		and	sot.ogr_id = ogr.ogr_id(+)
		and	spl.par_id = hsp.par_id
		and	spl.sop_id = hsp.sop_id_sortplan
		and	spl.sop_type = hsp.sop_type_sortplan
		and	hsp.par_id = sse.par_id
		and	hsp.sse_id = sse.sse_id
	);
	$statement .= &$nameClause('l', 'sloc.name');
	$statement .= &$nameClause('L', 'rloc.name');
	$statement .= &$nameClause('s', 'spl.name');
	$statement .= &$nameClause('c', 'cop.name');
	$statement .= &$valueClause('p', 'oco.par_id', 1);


	sql->run($statement);

}

# ------------------------------------------------------------
$COMMANDS->{"tversions"} =
  {
	  alias => undef,
	  description => "Show db_version_info",
	  synopsis => 
	    [
	     ["pattern"],
	     ["-a", undef, "show all, not just sql"],
	     ["-l", undef, "show full version note"],
	     ],
	      routine => \&tversions
    };

sub tversions {
	my ($this, $argv, $opts) = @_;
	my $pattern = ".sql";
	$pattern = $argv->[0] if $argv->[0];
	$pattern ='%'.$pattern.'%';
	my $versionNote = $opts->{l} ? 
	  "version_note"
	    :"substr(version_note,1, instr(version_note,' ')) version_note";

	sql->run (qq(
		select 
			$versionNote,
			installed
		from db_version_info 
		where version_note like '$pattern'
		order by installed));
}

# ------------------------------------------------------------
$COMMANDS->{"find_sortplan"} =
  {
	  alias => "fspl",
	  description => "Show sortplans",
	  synopsis => 
	    [
	     [],
	     ["-L", "loc pattern" , "restrict sortplans in that location"],
	     ["-D", "dom pattern" , "restrict sortplans in that domain"],
	     ["-S", "sse pattern" , "restrict sortplans in that scheme"],
	     ["-P", "pln pattern" , "restrict sortplans by name"],
	     ["-M", "sma pattern" , "restrict sortplans by machine"],
	     ["-p", "par_id[,sop_id]" , "restrict by par_id and sop_id"],
	     ["-l", undef , "print details of the (last) sortplan"],
	     ],
	      routine => \&tsortplans
    };

sub asPattern {
	# convert to uppercase and handle *s and %s
	my ($p) = @_;
	return "%" unless $p;
	my $pp = "%".$p."%";
	$pp =~ s/[\*%]*/%/;
	return $pp;
}

sub tsortplans {
	my ($this, $argv, $opts) = @_;
	my $locPattern = asPattern(uc($opts->{L}));
	my $domPattern = asPattern(uc($opts->{D}));
	my $ssePattern = asPattern(uc($opts->{S}));
	my $plnPattern = asPattern(uc($opts->{P}));
	my ($parId, $sopId) = split(",",$opts->{p});
	my $parClause = $parId ? "and spl.par_id=".$parId : "";
	my $sopClause = $sopId ? "and spl.sop_id=".$sopId : "";
	my $smaClause;
	$smaClause = qq(
		and exists (select 1 from ivo, sma
			where ivo.par_id = spl.par_id
			and ivo.sop_id_sortplan = spl.sop_id
			and ivo.sop_type_sortplan = spl.sop_type
			and ivo.par_id = sma.par_id
			and ivo.sma_id = sma.sma_id
			and upper(sma.name) like upper ('$opts->{M}')
		)
) if $opts->{M};

	my $lastRow = sql->run(qq(
		select 
			spl.par_id,
			loc.loc_id,
			loc.name location,
			dom.dom_id,
			dom.name domain,
			sse.sse_id,
			sse.name scheme,
			spl.sop_id, 
			spl.name sortplan
		from 
			sortplan_v spl,
			location_v loc,
			sorting_scheme_v sse,
			has_sortplan_v hso,
			domain_v dom
		where	1=1
		$parClause
		$sopClause
		$smaClause
		and	upper(loc.name) like '$locPattern'
		and	upper(dom.name) like '$domPattern'
		and	upper(sse.name) like '$ssePattern'
		and	upper(spl.name) like '$plnPattern'
		and	loc.par_id = dom.par_id
		and	loc.loc_id = dom.loc_id
		and	dom.par_id = sse.par_id
		and	dom.dom_id = sse.dom_id
		and	sse.par_id = hso.par_id
		and	sse.sse_id = hso.sse_id
		and	hso.par_id = spl.par_id
		and	hso.sop_id_sortplan = spl.sop_id
		and	hso.sop_type_sortplan = spl.sop_type
		order by 1,2,4,6
		));
	if ($opts->{l}) {
		print ("\n---- Plan $lastRow->[8] in $lastRow->[2]---");
		sql->run(qq(
	select
		SOP_ID,
		SOP_TYPE,
		MAS_ID,
		MAS_CODE,
		NAME,
		SOP_ID_RECIPIENT,
		SOP_TYPE_RECIPIENT,
		SOP_NAME_RECIPIENT,
		OCO_ID,
		CON_ID,
		CON_NAME,
		DOM_ID_RECIPIENT,
		DOM_TYPE_RECIPIENT,
		LOC_ID_RECIPIENT,
		LOC_NAME_RECIPIENT,
		DOM_NAME_RECIPIENT,
		OGR_ID,
		OGR_NAME
	from Sortout_Detailed_V
		where par_id = $lastRow->[0]
		and sop_id_sortplan = $lastRow->[7]

	));
	}

}

# ------------------------------------------------------------
$COMMANDS->{"count_sortplans"} =
  {
	  alias => "cspl",
	  description => "Count sortplans per (first) machine",
	  synopsis => 
	    [
	     [],
	     ["-L", "loc pattern" , "restrict sortplans in that location"],
	     ["-D", "dom pattern" , "restrict sortplans in that domain"],
	     ["-S", "sse pattern" , "restrict sortplans in that scheme"],
	     ["-P", "pln pattern" , "restrict sortplans  by pattern"],
	     ["-T", "totals(plds)" , " aggregate on par_id, location, domain or scheme"],
	     ["-p", "par_id" , "restrict by par_id"],
	     ],
	   man=>qq(
Count matching Sortplans per first machine and print some aggregated
	totals
),
	      routine => \&tcsortplans
    };


sub tcsortplans {
	my ($this, $argv, $opts) = @_;
	my $locPattern = asPattern(uc($opts->{L}));
	my $domPattern = asPattern(uc($opts->{D}));
	my $ssePattern = asPattern(uc($opts->{S}));
	my $plnPattern = asPattern(uc($opts->{P}));
	my $parClause = $opts->{p} ? "and spl.par_id=".$opts->{p} : "";
	my $aggLetters = $opts->{T} ? $opts->{T} : "ds";
	my @aggNumbers;
		if ($aggLetters =~ /p/i) { push(@aggNumbers, 8);}
		if ($aggLetters =~ /l/i) { push(@aggNumbers, 4);}
		if ($aggLetters =~ /d/i) { push(@aggNumbers, 2);}
		if ($aggLetters =~ /s/i) { push(@aggNumbers, 1);}
	my $aggClause = join(",",@aggNumbers);

	$aggLetters =~ s/[plds]//g;
	if ($aggLetters ne "") {
		print "Bad option -T $aggLetters\n";
		return;
	}
	#print "$aggLetters, $aggClause\n"; 

	sql->run(qq( 
	select * from (
		select 
			spl.par_id,
			loc.loc_id,
			loc.name location,
			dom.dom_id,
			dom.name domain,
			sse.sse_id,
			sse.name scheme,
			sma.name first_machine,
			count(*) sortplans
		from 
			sortplan_v spl,
			location_v loc,
			sorting_scheme_v sse,
			has_sortplan_v hso,
			domain_v dom,
			sorting_machine sma
		where	1=1
		$parClause
		and	upper(loc.name) like '$locPattern'
		and	upper(dom.name) like '$domPattern'
		and	upper(sse.name) like '$ssePattern'
		and	upper(spl.name) like '$plnPattern'
		and	loc.par_id = dom.par_id
		and	loc.loc_id = dom.loc_id
		and	dom.par_id = sse.par_id
		and	dom.dom_id = sse.dom_id
		and	sse.par_id = hso.par_id
		and	sse.sse_id = hso.sse_id
		and	hso.par_id = spl.par_id
		and	hso.sop_id_sortplan = spl.sop_id
		and	hso.sop_type_sortplan = spl.sop_type
		and	sma.par_id = spl.par_id
		and	sma.sma_id = (select min(sma_id) 
				   from ivo 
				   where par_id=spl.par_id
				   and	 sop_id_sortplan = spl.sop_id
				   and	 sop_type_sortplan=spl.sop_type)
		group by cube (
			spl.par_id,
			loc.loc_id,
			loc.name,
			dom.dom_id,
			dom.name,
			sse.sse_id,
			sse.name,
			sma.name)
		)
		where 
		1=1
		and decode(loc_id,null,-1,1) * decode(location,null,-1,1) = 1
		and decode(dom_id,null,-1,1) * decode(domain,null,-1,1) = 1
		and decode(sse_id,null,-1,1) * decode(scheme,null,-1,1) = 1
		and ((
		first_machine is not null
		and decode(par_id, null,0,8) + decode(loc_id, null,0,4) + decode(dom_id, null,0,2) + decode (sse_id, null,0,1)
--		in (8,12,14,15)
--		in (15,14)
		in ($aggClause)
		)
		or 
		(par_id is null and loc_id is null and dom_id is null and sse_id is null and first_machine is null)
		)
	order by 1,2,3,4,5,6,7
));
}

# ------------------------------------------------------------
$COMMANDS->{"tlog"} =
  {
   alias => undef,
   description => "Show trace_log entries",
   synopsis => 
   [
    [],
    ["-t", "n" , "show last n rows"],
    ["-r", "id" , "rows around id"],
    ["-s", "sid" , "restrict to sid"],
    ["-D", undef, "truncate trace log"],
    ["-p", "patterm" , "show matching rows"],
    ["-T", "table", "use table instead of trace_log"],
    ["-L", "level" , "set session trace level"],
    ["-1", "table" , "Create trigger to turn tracing on"],
    ["-0", "table" , "Create trigger to turn tracing off"]
   ],
   
   routine => \&tlog
    };

sub tlog {
	my ($this, $argv, $opts) = @_;

	my $table = "TRACE_LOG";
	$table = $opts->{T} if $opts->{T};

	if ($opts->{D}) {
		sql->run (qq(truncate table $table));
	}

	if ((my $level = $opts->{L}) && ! $opts->{1}) {
		sql->run(qq(begin db_trace.set_trace_level($level); end;));
		return;
	}
	my $sidClause = "";
	if ($opts->{s}) {
		my $sid = $opts->{s};
		$sidClause = qq(and trc_session like '$sid,%');
	}
	if (my $table = $opts->{1}) {
	        my $level = $opts->{L} ? $opts->{L} : 3;
		sql->run("drop trigger senora_trace_hack");
		my $stmt = qq(
create or replace trigger senora_trace_hack
after insert or update or delete on $table
begin
	db_trace.set_trace_level($level);
	db_trace.trace(1, 'SENORA','trace','tracing enabled by senora');
end;);
		sql->run($stmt);
		return;
	}

	if (my $table = $opts->{0}) {
		sql->run("drop trigger senora_trace_hack");
		my $stmt = qq(
create or replace trigger senora_trace_hack
after insert or update or delete on $table
begin
	db_trace.trace(1, 'SENORA','trace','tracing disabled by senora');
	db_trace.set_trace_level(0);
end;);
		sql->run($stmt);
		return;
	}
	if (my $nrows = $opts->{t}) {
		sql->run (qq(
		select * from $table
		where trc_id > (select max(trc_id) from $table) - $nrows
		$sidClause
		order by trc_id
		));
		return;
	}
	if (my $pattern = uc($opts->{p})) {
		$pattern = asPattern($pattern);
		sql->run (qq(
		select * from $table
		where upper(trc_message) like '$pattern'
		$sidClause
		order by trc_id
		));
		return;
	}

	if (my $id = $opts->{r}) {
		sql->run (qq(
		select 
			decode(TRC_ID, $id, '*****', trc_id) id       ,
			TRC_LEVEL    ,
			TRC_TIMESTAMP,
			TRC_SESSION  ,
			TRC_MESSAGE   
		from $table
		where trc_id between $id-20 and $id+40
		$sidClause
		order by trc_id
		));
		return;
	}
	sql->run("select count(*) trace_rows from $table");
}
# ------------------------------------------------------------
$COMMANDS->{"dplSoc"} =
  {
   alias => undef,
   description => "Show DPL contract products connected to sortcode",
   synopsis => 
   [
    ["sortcode"],
    ["-c", undef, "only count COPs per CON"],
    ["-b", undef, "only report multiple cops ('ok/bad')"],
   ],
   
   routine => \&followSoc
    };

sub followSoc {
	my ($this, $argv, $opts) = @_;
	my $sortcode = $argv->[0];
	my $columns;
	my $grouping;

	$columns="distinct sop.par_id, sop.sop_id,sop.sop_type,sop.name, sop.con_id";
	$grouping="order by sop.par_id, sop.con_id";

	if ($opts->{c}) {
		$columns="sop.par_id, sop.con_id, count(distinct sop.sop_id) COPS";
		$grouping="group by sop.par_id, sop.con_id";
	}
	if ($opts->{b}) {
		$columns="decode(max(count(distinct sop.sop_id)),0,'ok',1,'  ok   ','**bad**')||' $sortcode' status";
		$grouping="group by sop.par_id, sop.con_id";
	}


	sql->run(qq(
		select $columns
		from 
			sop,
			lot,
			(
			select
				par_id, sop_id_super, sop_type_super
			from ctp
			start with
				(par_id, sop_id, sop_type) in (
					select par_id, sop_id, sop_type
					from address_delivery_product_v
					where (par_id, soc_id) in (
						select par_id, soc_id
						from sortcode_v
						where value = '$sortcode'
					)
				)

			connect by 
				prior par_id = par_id
			and	prior sop_id_super = sop_id
			and	prior sop_type_super = sop_type
			) ctp
		where
			sop.par_id = lot.par_id
		and	sop.sop_id = lot.sop_id
		and	sop.sop_type = lot.sop_type
		and 	lot.par_id = ctp.par_id
		and	lot.sop_id_recipient = ctp.sop_id_super
		and	lot.sop_type_recipient = ctp.sop_type_super
		$grouping
	));
}
# ------------------------------------------------------------
$COMMANDS->{"explainCode"} =
  {
   alias => undef,
   description => "Examine the inheritance path of a Sortcode",
   synopsis => 
   [
    ["sortcode"],
    ["-p", "parId" , "par_id of the realease (1)"],
    ["-l", "match" , "Pattern matching the name of the sender Location"],
    ["-d", "match" , "Pattern matching the name of the sender Domain"],
    ["-L", "match" , "Pattern matching the name of the reciver Location"],
    ["-D", "match" , "Pattern matching the name of the reciver Domain"],

   ],
   
   routine => \&explainCode
    };

sub explainCode {
	my ($this, $argv, $opts) = @_;
	my $parId = $opts->{p} ? $opts->{p} : 1;
	my $soc = $argv->[0];

	# ------------------------------------------------------------
	my $dom = $this->getDomainOfSoc($parId, $soc);
	unless (@$dom) {
		print "Sortcode $soc does not exist or has no ADP. \n";
		return;
	}
	my $dlocName = $dom->[0]->[9];
	my $ddomName = $dom->[0]->[3];
	printf ("Sortcode $soc is delivered by \"%s/%s\".\n", $dlocName,$ddomName);
	print "\n";

	my $slocName = $opts->{l} ? $opts->{l} : '%';
	my $sdomName = $opts->{d} ? $opts->{d} : '%';
	# Use delivery domain as receiver unless a sender location was sepcified
	my $rlocName = $opts->{L} ? $opts->{L} : ($opts->{l} ? '%' : $dlocName);
	my $rdomName = $opts->{D} ? $opts->{D} : ($opts->{l} ? '%' : $ddomName);

	sql->run(qq(
		select sloc.name sloc, sdom.name sdom, rloc.name rloc, rdom.name rdom, oco.oco_id, con.name contract, cop.name cop, cop.sop_id, ext.mal_id 
		from
			location sloc,
			domain sdom,
			outline_contract oco,
			location rloc,
			domain rdom,
			contract con,
			contract_product_v cop,
			expands_to ext
		where
			rloc.par_id = $parId
		and	rloc.name like '$rlocName'
		and 	rdom.name like '$rdomName'
		and 	sloc.name like '$slocName'
		and	rloc.par_id = rdom.par_id
		and	rloc.loc_id = rdom.loc_id
		and	rdom.par_id = oco.par_id
		and	rdom.dom_id = oco.dom_id_recipient
		and	oco.par_id = sdom.par_id
		and	oco.dom_id_sender = sdom.dom_id
		and	sdom.par_id = sloc.par_id
		and	sdom.loc_id = sloc.loc_id
		--
		and	oco.par_id = con.par_id
		and	oco.oco_id = con.oco_id
		and	con.par_id = cop.par_id
		and	con.con_id = cop.con_id
		and	cop.par_id = ext.par_id
		and	cop.sop_type = ext.sop_type
		and	cop.sop_id = ext.sop_id
		and	(
			select num_value from sortcode where par_id=$parId 
			and value = '$soc'
			) between ext.sortcode_low and ext.sortcode_high
		order by 1,2,3,4,5,6,7
	));

}

sub getDomainOfSoc {
	my ($this, $parId, $soc) = @_;
	my $stmt = qq(
		select
			distinct dom.*
		from 
			sortcode soc,
			address_delivery_product_v adp,
			domaindto_v dom
		where	soc.par_id = $parId
		and	soc.value='$soc'
		and	soc.par_id = adp.par_id
		and	soc.soc_id = adp.soc_id
		and	adp.par_id = dom.dom_par_id
		and	adp.dom_id = dom.dom_dom_id
	);
	return sql->runArrayref($stmt);
}

sub getDeliveryExports {
	my ($this, $parId, $soc, $dom) = @_;
	my $stmt = qq(
		select
			cop.*
		from 
			Contractproductlinkeddto_V cop,
			expands_to ext,
			sortcode soc
		where	1=1
		and soc.par_id = $parId
		and soc.value = '$soc'
		and ext.par_id = soc.par_id
		and	soc.num_value between ext.sortcode_low and ext.sortcode_high
		and	ext.par_id = cop.cop_par_id
		and	ext.sop_id = cop.cop_sop_id
		and	ext.sop_type = cop.cop_sop_type
		and	cop.cop_par_id = $dom->[0]->[0]
		and	cop.rcv_dom_id = $dom->[0]->[1]
		order by 4,16
		);
	return sql->runArrayref($stmt);
}

sub getLotsToCop {
	my ($parid, $sopid) = @_;
	my $stmt = qq(
		select count(*)
		from lot
		where par_id = $parid
		and sop_id_recipient = $sopid
		and sop_type_recipient = 'C'
	);
	return sql->runArrayref($stmt)->[0]->[0];
}

# ------------------------------------------------------------
$COMMANDS->{"sortplanDetails"} =
  {
   alias => "spd",
   description => "Show ranges and stackers of a sortplan",
   synopsis => 
   [
    [],
    ["-L", "loc pattern" , "restrict sortplans in that location"],
    ["-P", "pln pattern" , "restrict sortplans in that name"],
    ["-i", "spl_id" , "provide the spl_id right away"],
    ["-d", "sclength" , "truncate sortcode"],
    ["-o", "outlet" , "restrict to outlet"],
    ["-p", "parId" , "par_id of the realease (1)"],
   ],
   
   routine => \&sortplanDetails
    };
sub sortplanDetails {
	my ($this, $argv, $opts) = @_;


	my $spl_name_clause = "";
	if ($opts->{P}){
	   	$spl_name_clause = "and spl.name like '" . $opts->{P} . "'";
	}	
	my $spl_id_clause = "";
	if ($opts->{i}) {
		$spl_id_clause = "and spl.sop_id = " . $opts->{i};
	}

	if ($spl_name_clause . $spl_id_clause eq "") {
		print "No Sorplan specified\n";
		return;
	}
	my $digits = 16;
	if ($opts->{d}) {
		$digits = $opts->{d}
	}
	my $ogr_pattern;
	if ($opts->{o}){
		$ogr_pattern = "and ogr.name like '" . $opts->{o}."'";
	}
	my $parId = $opts->{p} ? $opts->{p} : 1;


	my $stmt = qq(
		select --+ xFIRST_ROWS
			loc_name, --0
			dom_name, --1
			spl_name, --2
			ogr_name, --3
			ogr_id, --4
			sop_id, --5
			mal_id, --6
			substr(sortcode_low,1,$digits)||'~'||substr(sortcode_high,1,$digits) range, --7
			scf_id --8
		from (
			select distinct
				loc.name loc_name,
				dom.name dom_name,
				spl.name spl_name,
				sot.sop_id sop_id,
				ogr.name ogr_name,
				ogr.ogr_id ogr_id,
				ext.mal_id,
				ext.sortcode_low,
				ext.sortcode_high,
				ext.scf_id
			from
				sortplan_v spl,
				domain dom,
				location loc,
				sortout sot,
				expands_to_base_v ext,
				outlet_group ogr
			where	spl.par_id = $parId
			and	spl.par_id = dom.par_id
			and	spl.dom_id = dom.dom_id
			and	dom.par_id = loc.par_id
			and	dom.loc_id = loc.loc_id
			------------------------------
			and	sot.par_id = spl.par_id
			and	sot.sop_id_sortplan = spl.sop_id
			and	sot.sop_type_sortplan = spl.sop_type
			and	ext.par_id = sot.par_id
			and	ext.sop_id = sot.sop_id
			and	ext.sop_type = sot.sop_type
			------------------------------
			and	sot.par_id = ogr.par_id (+)
			and	sot.sop_id_sortplan = ogr.sop_id_sortplan (+)
			and	sot.sop_type_sortplan = ogr.sop_type_sortplan (+)
			and	sot.ogr_id = ogr.ogr_id (+)
			$spl_name_clause
			$spl_id_clause
		        $ogr_pattern
		)
		order by range, scf_id, mal_id
		);
	my $sth = sql->prepare($stmt);
	$sth->execute();

	my $format="%-8s %-6s %-6s %-8s %-4s %s";
	my $row;
	my $prevRow;
	print "----------------------------------------------------\n";
	while ($row = $sth->fetchrow_arrayref) {
		unless ($prevRow) {
			# print header
			printf ("%-16s: %s\n","Location", $row->[0]);
			printf ("%-16s: %s\n","Domain", $row->[1]);
			printf ("%-16s: %s\n","Sortplan", $row->[2]);
			if ($opts->{d}) {
				printf ("%-16s: %s\n","Digits", $opts->{d});
			}
			printf ($format, "OgrName", "OgrId", "MAL", "SopId", "scf_id", "Range");
			print "\n";
			my $prevRow;
			print "----------------------------------------------------\n";
		}
		printf ($format, $row->[3], $row->[4], $row->[6], $row->[5], $row->[8], $row->[7]);
		if (rangesOverlap($row, $prevRow)) {
			print " overlap\n";
		} else {
			print "\n";			
		}
		    
		# copy + remember prev row
		my @x = @$row;
		$prevRow = \@x;
	}
}

sub rangesOverlap {
	my ($row, $prevRow) = @_;

	unless ($prevRow) {
		return 0;
	}
	my @prevRange = split("~", $prevRow->[7]);
	my @currRange = split("~", $row->[7]);
	return ($prevRange[1] cmp $currRange[0]) > 0;
}


# ------------------------------------------------------------
$COMMANDS->{"senderOf"} =
  {
   alias => undef,
   description => "Show where mail comes from recursively",
   synopsis => 
   [
    [],
    ["-p", "par_id", ""],
    ["-s", "sop_id", ""],
    ["-t", "sop_type", ""],
   ],
   
   routine => \&senderOf
    };

sub senderOf {
	my ($this, $argv, $opts) = @_;

	my $par_id = 1;
	$par_id = $opts->{p} if ($opts->{p});
	my $sop_id = $opts->{"s"};
	my $sop_type = $opts->{t};
	sendersOfLoop ($par_id, $sop_id, $sop_type);
}

sub sendersOfLoop {
	my ($par_id, $sop_id, $sop_type) = @_;

	my $senders = getSenders($par_id, $sop_id, $sop_type);
	foreach my $sender (@$senders) {
		my $s;
		if ($sender->[2] eq 'I') {
			$s = getSendingSortplan($sender->[0], $sender->[1])->[0];
		} elsif ($sender->[2] eq 'C') {
			$s = getSendingCop($sender->[0], $sender->[1])->[0];
		}
		my $sndIds = "($sender->[0],$sender->[1],$sender->[2])";
		printf ("%-12s %-30s %-30s %-30s\n", $sndIds, $s->[3], $s->[4], $s->[5]);
		print "follow further (y/n/q)? ";
		my $answer = <>;
		chomp $answer;
		if (uc($answer) eq "Y") {
			sendersOfLoop($sender->[0], $sender->[1], $sender->[2]);
		}
	}
}

sub getSenders {
	my ($par_id, $sop_id, $sop_type) = @_;

	my $stmt = qq(
		select par_id, sop_id, sop_type
		from local_transport
		where par_id = $par_id
		and sop_id_recipient = $sop_id
		and sop_type_recipient = '$sop_type'
       );
	return sql->runArrayref($stmt);
}

sub getSendingSortplan {
	my ($par_id, $sop_id) = @_;
	
	my $stmt = qq(
		select
			spl.par_id,
			spl.sop_id,
			spl.sop_type,
			loc.name loc_name,
			dom.name dom_name,
			spl.name spl_name
		from
			sortout sot,
			sortplan_v spl,
			domain dom,
			location loc
		where
			sot.par_id = $par_id
		and	sot.sop_id = $sop_id
		and	sot.sop_type = 'I'
		and	sot.par_id = spl.par_id
		and	sot.sop_id_sortplan = spl.sop_id
		and	sot.sop_type_sortplan = spl.sop_type
		and	spl.par_id = dom.par_id
		and	spl.dom_id = dom.dom_id
		and	dom.par_id = loc.par_id
		and	dom.loc_id = loc.loc_id
		);
	return sql->runArrayref($stmt);
}

sub getSendingCop {
	my ($par_id, $sop_id) = @_;
	my $stmt = qq(
		select
			cop_par_id,
			cop_sop_id,
			cop_sop_type,
			snd_loc_name ||' ('||snd_name||')',
			con_name,
			cop_name
		from
			contractproductlinkeddto_v
		where
			cop_par_id = $par_id
		and	cop_sop_id = $sop_id
		and	cop_sop_type = 'C'
	);
	return sql->runArrayref($stmt);
}



# ------------------------------------------------------------
$COMMANDS->{"emptySections"} =
  {
   alias => undef,
   description => "Show empty sortplan sections",
   synopsis => 
   [
    [],
    ["-L", "loc pattern" , "restrict sortplans in that location"],
    ["-P", "pln pattern" , "restrict sortplans in that name"],
   ],
   
   routine => \&emptySections
    };

sub emptySections {
	my ($this, $argv, $opts) = @_;

	my $locPattern = asPattern(uc($opts->{L}));
	my $plnPattern = asPattern(uc($opts->{P}));

	my $stmt = qq(
		select
			loc.name location, 
			spl.name sortplan,
			sec.name section,
			lot.sop_id sender
		from
			location loc,
			domain dom,
			sortplan_v spl,
			sortplan_section_v sec,
			local_transport lot
		where	1=1
		and	loc.par_id = dom.par_id
		and	loc.loc_id = dom.loc_id
		-- -- --
		and	dom.par_id = spl.par_id
		and	dom.dom_id = spl.dom_id
		and 	dom.dom_type ='S'
		-- -- --
		and	spl.par_id = sec.par_id
		and	spl.sop_id = sec.sop_id_sortplan
		and	spl.sop_type = sec.sop_type_sortplan
		-- -- --
		and	sec.par_id = lot.par_id(+)
		and	sec.sop_id = lot.sop_id_recipient (+)
		and	sec.sop_type = lot.sop_type_recipient (+)
		----------------------------------------
		and not exists (
			select 1 from sortout sot
			where sot.par_id = sec.par_id
			and sot.sop_id_section = sec.sop_id
			and sot.sop_type_section = sec.sop_type
		)
		----------------------------------------
		and 	upper(loc.name) like '$locPattern'
		and	upper(spl.name) like '$plnPattern'
		order by 1,2,3
);
	sql->run($stmt);
}



# ------------------------------------------------------------
$COMMANDS->{"whyIsCodeInSortplan"} =
  {
   alias => "wicis",
   description => "Print an explanation why a code is in a sortplan",
   synopsis => 
   [
    [],
    ["-p", "par_id" , "Release slot"],
    ["-L", "loc pattern" , "restrict sortplans in that location"],
    ["-P", "pln pattern" , "restrict sortplans in that name"],
    ["-C", "sortcode" , "Find this code"],
   ],
   
   routine => \&wicis
    };

sub wicis {
	my ($this, $argv, $opts) = @_;
	print("11\n");
	my $parId = $opts->{p} ? $opts->{p} : 1;
	my $locPattern = asPattern(uc($opts->{L}));
	my $plnName = $opts->{P};
	my $sortcode = $opts->{C} or return;
	settings->push();
	columnFormat->set('COP_PAR_ID', 'a12');
	columnFormat->set('PAR_ID', 'a7');
	columnFormat->set('COP_SOP_ID', 'a12');
	columnFormat->set('COP_SOP_Type', 'a12');
	columnFormat->set('CON_CON_id', 'a12');
	columnFormat->set('CON_NAME', 'a20');
	columnFormat->set('SND_LOC_NAME','a20');
	columnFormat->set('SND_NAME','a20');
	columnFormat->set('RCV_LOC_NAME','a20');
	columnFormat->set('RCV_NAME','a20');

	setting->set("Feedback", "OFF");
	my ($parId, $sopId) = $this->wicisFindSortplan($parId, $locPattern, $plnName);
	print ("\n$line Flow analysis\n$line");
	my $inps = $this->wicisRecurseCodeInSortplan($parId, $sopId, $sortcode);

	if ($inps->[0]) {
		print ("\n$line Contract Product inheritance\n$line");
		foreach my $inp (@$inps) {
			$this->wicisCopAnalysis ($parId, $inp->[1], 'I', $sortcode);
		}
	} else {
		print "Sortcode not found in sortplan.\n";
	}

	settings->pop();
}


sub wicisRecurseCodeInSortplan {
	my ($this, $parId, $sopId, $sortcode) = @_;
	if ($this->isManualSortplan ($parId, $sopId)) {
		print ("\nCannot look beyond  manual sortplans.\n");
		return;
	}
	my $inps = $this->wicisCodeInSortplan($parId, $sopId, $sortcode);

	foreach my $row (@$inps) {
		my ($outName, $ssop, $foo, $bar, $scLow, $scHigh) = @$row;
		print ("\n*** Outlet = $outName, Sop_id = $ssop, scLow = $scLow, scHigh = $scHigh ***\n\n");

		my ($dsop, $dtype) = $this->wicisGetDestOfSop($parId, $ssop, 'I');
		print ("=> $ssop Flows to ($dsop,$dtype)\n");
		$this->explainSop ($parId, $dsop, $dtype);
		if ($dtype eq 'P') {
			$this->wicisRecurseCodeInSortplan($parId, $dsop, $sortcode);
		}
		if ($dtype eq 'E') {
			my $splId = $this->wicisSortplanOfSection($parId, $dsop);
			$this->wicisRecurseCodeInSortplan($parId, $splId, $sortcode);
		}
		if ($dtype eq 'C') {
			my $ssop = $dsop;
			my ($dsop, $dtype) = $this->wicisGetDestOfSop($parId, $ssop, $dtype);
			print ("=> $ssop Flows to ($dsop,$dtype)\n");
			$this->explainSop ($parId, $dsop, $dtype);
			if ($dtype eq 'P') {
				$this->wicisRecurseCodeInSortplan($parId, $dsop, $sortcode);
			}
		}
	}
	return $inps;
}

sub isManualSortplan {
	my ($this, $parId, $sopId) = @_;

	my $rs = sql->runArrayref (qq(
		select count(*)
		from 
			is_valid_on ivo,
			sorting_machine sma
		where
			ivo.par_id = sma.par_id
		and	ivo.sma_id = sma.sma_id
		and	upper(sma.name) = 'MANUAL'
		and	ivo.par_id = $parId
		and	ivo.sop_id_sortplan = $sopId
		and	ivo.sop_type_sortplan = 'P'
	));
	return (($rs->[0]->[0]) > 0);
}

sub explainSop {
	my ($this, $parId, $sopId, $sopType) = @_;
	if ($sopType eq 'I') {
		sql->run(qq(
			select 
				sop_id_recipient,
				sop_type_recipient,
				sop_name_recipient,
				con_name,
				loc_name_recipient,
				dom_name_recipient
			from
				Sortout_Detailed_V
			where
				par_id = $parId
			and	sop_id = $sopId
			and	sop_type = $sopType
			));
		return;
	}
	if ($sopType eq 'C') {
		sql->run(qq(
			select
				cop_par_id par_id,
				cop_sop_id,
				--cop_sop_type,
				con_con_id,
				con_name,
				cop_name,
				snd_loc_name,
				snd_name,
				rcv_loc_name,
				rcv_name
			from
				Contractproductlinkeddto_V
			where
				cop_par_id = $parId
			and	cop_sop_id = $sopId
			and	cop_sop_type = '$sopType'
	));
		return;
	}
	if ($sopType eq 'P') {
		sql->run(qq(
			select
				spl.par_id,
				spl.sop_id,
				loc.name loc_name,
				dom.name dom_name,
				spl.name pln_name
			from 
				sortplan_v spl,
				domain dom,
				location loc
			where
				spl.par_id = dom.par_id
			and	spl.dom_id = dom.dom_id
			and	dom.par_id = loc.par_id
			and	dom.loc_id = loc.loc_id
			and spl.par_id = $parId
			and spl.sop_id = $sopId
			and spl.sop_type = '$sopType'
      		));
		return;
	}
	if ($sopType eq 'G') {
		sql->run(qq(
			select
				pgr.par_id,
				pgr.sop_id,
				pgr.sop_type,
				pgr.name pgr_name,
				loc.name loc_name,
				dom.name dom_name,
				sse.name sse_name
			from
				product_group_v pgr,
				domain dom,
				location loc,
				sorting_scheme sse
			where
				pgr.par_id = dom.par_id
			and	pgr.dom_id = dom.dom_id
			and	dom.par_id = loc.par_id
			and	dom.loc_id = loc.loc_id
			and	pgr.par_id = sse.par_id
			and	pgr.sse_id = sse.sse_id
			and pgr.par_id = $parId
			and pgr.sop_id = $sopId
			and pgr.sop_type = '$sopType'
		));
		return;
	}
	if ($sopType eq 'E') {
		sql->run(qq(
			select
				sps.par_id,
				sps.sop_id,
				sps.sop_type,
				spl.sop_id spl_id,
				sps.name sec_name,
				spl.name spl_name,
				loc.name loc_name,
				dom.name dom_name
			from
				sortplan_section_v sps,
				sortplan_v spl,
				location loc,
				domain dom
			where
				sps.par_id = spl.par_id
			and	sps.sop_id_sortplan = spl.sop_id
			and	sps.sop_type_sortplan = spl.sop_type
			and	spl.par_id = dom.par_id
			and	spl.dom_id = dom.dom_id
			and	dom.par_id = loc.par_id
			and	dom.loc_id = loc.loc_id
			and sps.par_id = $parId
			and sps.sop_id = $sopId
			and sps.sop_type = '$sopType'
		));
		return
	}
	print ("Cannont explain SOPs of type '$sopType'\n");
}

sub wicisFindSortplan {
	# Find sortplan by Location and Sortplan name
	my ($this, $parId, $locPattern, $plnName) = @_;
	my $stmt = qq(
		select
			spl.par_id,
			spl.sop_id,
			loc.name loc_name,
			dom.name dom_name,
			spl.name pln_name
		from 
			sortplan_v spl,
			domain dom,
			location loc
		where	spl.par_id = $parId
		and	spl.par_id = dom.par_id
		and	spl.dom_id = dom.dom_id
		and	dom.par_id = loc.par_id
		and	dom.loc_id = loc.loc_id
		and	spl.name = '$plnName'
		and	loc.name like '$locPattern'
	);
	sql->run ($stmt);
	my $row = sql->runArrayref($stmt);
	return ($row->[0]->[0], $row->[0]->[1]);
}

sub wicisCodeInSortplan {
	my ($this, $parId, $sopId, $sortcode) = @_;

	my $stmt = qq(
			select distinct
				out.name out_name,
				sot.sop_id sop_id,
				out.seq_num out_number,
				ext.mal_id,
				ext.sortcode_low,
				ext.sortcode_high
			from
				sortout sot,
				expands_to_base_v ext,
				groups_outlets gro,
				outlet out
			where   1=1
			and	sot.par_id = $parId
			and	sot.sop_id_sortplan = $sopId
			and	sot.sop_type_sortplan = 'P'
			and	ext.par_id = sot.par_id
			and	ext.sop_id = sot.sop_id
			and	ext.sop_type = sot.sop_type
			------------------------------
			and	sot.par_id = gro.par_id
			and	sot.sop_id_sortplan = gro.sop_id_sortplan
			and	sot.sop_type_sortplan = gro.sop_type_sortplan
			and	sot.ogr_id = gro.ogr_id
			and	gro.par_id = out.par_id
			and	gro.out_id = out.out_id
			and	gro.sma_id = out.sma_id
			------------------------------
			and '$sortcode' between ext.sortcode_low and ext.sortcode_high
		);
	my $rs = sql->runArrayref($stmt);
	my @sops;
	foreach my $row (@$rs) {
		push (@sops, $row);
	}
	return \@sops;
}

sub wicisGetDestOfSop {
	my ($this, $parId, $sopId, $sopType) = @_;
	my $stmt = qq(
		select
			par_id,
			sop_id_recipient,
			sop_type_recipient
		from
			local_transport
		where
			par_id = $parId
		and	sop_id = $sopId
		and	sop_type = '$sopType'
	);

	my $rs = sql->runArrayref($stmt);
	return ($rs->[0]->[1],$rs->[0]->[2]);
}

sub wicisSortplanOfSection {
	my ($this, $parId, $sopId) = @_;
	my $rs = sql->runArrayref (qq(
		select sop_id_sortplan
		from has_sortplan_section
		where par_id = $parId
		and sop_id = $sopId
		and sop_type = 'E'
	));
	return $rs->[0]->[0];
}

sub wicisCopAnalysis {
	my ($this, $parId, $sopId, $sopType, $sortcode) = @_;

	my $rows = $this->wicisGetInfCop($parId, $sopId, $sopType, $sortcode);
	foreach my $row (@$rows) {
		my ($parId, $sopId) = @$row;
		$this->wicisCopAnalysis ($parId, $sopId, 'C', $sortcode) if $parId;
	}


}

sub wicisGetInfCop {
	# get the COPs that provide sorcode
	my ($this, $parId, $sopId, $sopType, $sortcode) = @_;
	my $stmt = qq(
			select distinct
				cop_par_id par_id,
				cop_sop_id,
				-- cop_sop_type,
				con_con_id,
				con_name,
				snd_loc_name,
				snd_name,
				rcv_loc_name,
				rcv_name
			from
				is_net_composed_from inf,
				expands_to ext,
				contractproductlinkeddto_v cpl
			where	1=1
			and	inf.par_id = ext.par_id
			and	inf.sop_id_contract = ext.sop_id
			and	inf.sop_type_contract = ext.sop_type
			and	inf.par_id = cpl.cop_par_id
			and	inf.sop_id_contract = cpl.cop_sop_id
			and	inf.sop_type_contract = cpl.cop_sop_type
			and	inf.par_id = $parId
			and	inf.sop_id = $sopId
			and	inf.sop_type = '$sopType'
			and	'$sortcode' between ext.sortcode_low and ext.sortcode_high
	);
	sql->run ($stmt);
	return sql->runArrayref ($stmt);
}


# ------------------------------------------------------------
$COMMANDS->{"mailAttributeLine"} =
  {
   alias => "mal",
   description => "Print the meaning of a mail attribute line",
   synopsis => 
   [
    [],
    ["-p", "parId" , "select par_id (1)"],
    ["-m", "malIds" , "comma separated list of mals"],
   ],
   
   routine => \&mailAttributeLine
    };


sub mailAttributeLine {
	my ($this, $argv, $opts) = @_;
	my $parId = $opts->{"p"} ? $opts->{"p"} : 1;
	my $malIds = $opts->{"m"};
	my $malIdClause = $opts->{"m"} ? "and mal_id in ($malIds)" :"";

	sql->run(qq(
		select 
			malc.mal_id, 
			mac.mac_id, 
			mac.code, 
			mat.mat_id, 
			mat.code
		from
			MAL_CONSISTS_OF malc,
			mail_attribute_category mac,
			MAIL_ATTRIBUTE mat
		where
			malc.par_id = $parId
		$malIdClause
		and	malc.par_id = mac.par_id
		and	malc.mac_id = mac.mac_id
		and	mac.par_id = mat.par_id
		and	mac.mac_id = mat.mac_id
		and	malc.mat_id = mat.mat_id
		order by malc.mal_id, mac.mac_id, mat.mat_id
));
}


# ------------------------------------------------------------
$COMMANDS->{"ambigousCodes"} =
  {
   alias => "ambc",
   description => "Find sortcodes whose initial chars occur in several domains",
   synopsis => 
   [
    [],
    ["-c", "chars" , "The number of chars to consider(8) "],
   ],
   
   routine => \&ambigousCodes
    };

sub ambigousCodes {
	my ($this, $argv, $opts) = @_;

	my $chars = $opts->{c} ? $opts->{c} : 8;

	print "wait ... \n";
	sql->run(qq(
		select
			loc.par_id,
			substr(soc.value,1,$chars) code,
			loc.name location,
			dom.name domain,
			dom.dom_id,
			count(*) occurrences
		from
			location loc,
			domain dom,
			address_delivery_product_v adp,
			sortcode soc,
			(
			-- get codes which occur in multiple domains
			select
				par_id,
				code,
				count(*) domains
			from (
				-- get substringed codes and their domains
				select 
					adp.par_id,
					adp.dom_id,
					substr (soc.value,1,$chars) code,
					count(*) ncodes
				from 
					sortcode soc,
					address_delivery_product_v adp
				where
					adp.par_id = soc.par_id
				and	adp.soc_id = soc.soc_id
				group by adp.par_id, adp.dom_id,  substr (soc.value,1,$chars)
			)
			group by par_id, code
			having count(*) > 1
			) dupes
		where
			dupes.par_id = soc.par_id
		and	dupes.code = substr(soc.value,1,$chars)
		and	soc.par_id = adp.par_id
		and	soc.soc_id = adp.soc_id
		and	adp.par_id = dom.par_id
		and	adp.dom_id = dom.dom_id
		and	dom.par_id = loc.par_id
		and	dom.loc_id = loc.loc_id
		group by 
			loc.par_id,
			substr(soc.value,1,$chars),
			loc.name,
			dom.name,
			dom.dom_id
		order by 1
	));
}



# ------------------------------------------------------------
$COMMANDS->{"sortingStrategy"} =
  {
   alias => undef,
   description => "Show the connections between sortplans",
   synopsis => 
   [
    [],
	     ["-l", "pattern", 'restrict sender location'],
	     ["-d", "pattern", 'restrict sender domain'],
	     ["-s", "pattern", 'restrict sender plan'],
	     ["-L", "pattern", 'restrict receiver location'],
	     ["-D", "pattern", 'restrict receiver domain'],
	     ["-S", "pattern", 'restrict receiver sortplan'],
	     ["-t", "conn_type", 'restrict connection type'],
	     ["-v", undef, 'show IDs'],
	     ["-p", "par_id", 'Release'],
   ],
   man=>qq(
	Show the connections between presorting and folloup
	sortplans. 

	There are serveral connection types:
	DOM-PLN is a connection between two plans in a Domain
	DOM-SEC is a conecction in a Domain to a Section
	CON-PLN is a connection between two plans across Domains
	CON-SEC is a connection across domains to a Section
),
   
   routine => \&sortingStrategy
    };

sub sortingStrategy {
	my ($this, $argv, $opts) = @_;
	my $senderLocation=asPattern($opts->{l});
	my $receiverLocation=asPattern($opts->{L});
	my $senderDomain=asPattern($opts->{d});
	my $receiverDomain=asPattern($opts->{D});
	my $senderPlan=asPattern($opts->{s});
	my $receiverPlan=asPattern($opts->{S});
	my $connType = asPattern($opts->{t});
	my $parId = $opts->{p} ? $opts->{p} : 1;

	my $projection;
	if ($opts->{v}) {
	   $projection=qq(
	       sloc.par_id,
	       sloc.loc_id sloc_id,
	       sloc.name sender_location,
	       sdom.dom_id sdom_id,
	       sdom.name sender_domain,
	       splan.sop_id sspl_id,
	       splan.name sender_plan,
	       rloc.loc_id rloc_id,
	       rloc.name receiver_location,
	       rdom.dom_id rdom_id,
	       rdom.name receiver_domain,
	       link.followup_sec_id,
	       rplan.sop_id rspl_id,
	       rplan.name receiver_plan,
	       link.conn_type
	);} else {
	   $projection=qq(
	       sloc.name sender_location,
	       sdom.name sender_domain,
	       splan.name sender_plan,
	       rloc.name receiver_location,
	       rdom.name receiver_domain,
	       rplan.name receiver_plan,
	       link.conn_type
	       );}

	sql->run(qq(
		select distinct 
		$projection
		from
			location  sloc,
			domain sdom,
			sortplan_v splan,
			location rloc,
			domain rdom,
			sortplan_v rplan,
			(select 
				par_id,
				presorting_sop_id,
				-- show plan of section
				(case 
				     when followup_sop_type = 'P'
					then followup_sop_id
				     when followup_sop_type = 'E'
					then ( 
						select sop_id_sortplan
						from has_sortplan_section hss
						where hss.par_id=innerLink.par_id
						and hss.sop_id =innerLink.followup_sop_id
						and hss.sop_type =innerLink.followup_sop_type
					)
				end ) followup_spl_id,
				(case 
				     when followup_sop_type = 'P'
					then NULL
				     when followup_sop_type = 'E'
				     	then followup_sop_id
				end ) followup_sec_id,
				'P' followup_sop_type,
				innerLink.conn_type || decode (followup_sop_type,'E','-SEC','P','-PLN','???') conn_type
			from
			(

				select
					-- connections in a domain
					sot.par_id par_id,
					sot.sop_id_sortplan presorting_sop_id,
					lot.sop_id_recipient followup_sop_id,
					lot.sop_type_recipient followup_sop_type,
					'DOM' conn_type
				from
					sortout_v sot,
					local_transport lot
				where
					-- local transport starts at product in sortout
					sot.par_id = $parId
				and	sot.par_id = lot.par_id
				and	sot.sop_id = lot.sop_id
				and	sot.sop_type = lot.sop_type
				and	lot.sop_type_recipient in ('P', 'E')
				-------------
				union all
				-------------
				select
					-- connections across domains
					sot.par_id par_id,
					sot.sop_id_sortplan presorting_sop_id,
					lot_rdom.sop_id_recipient followup_sop_id,
					lot_rdom.sop_type_recipient followup_sop_type,
					'CON' conn_type
				from
					sortout_v sot,
					local_transport lot_sdom,
					local_transport lot_rdom
				where
					-- local transport starts at product in sortout
					sot.par_id = $parId
				and	sot.par_id = lot_sdom.par_id
				and	sot.sop_id = lot_sdom.sop_id
				and	sot.sop_type = lot_sdom.sop_type
				and	lot_sdom.par_id = lot_rdom.par_id
				and	lot_sdom.sop_id_recipient = lot_rdom.sop_id
				and	lot_sdom.sop_type_recipient = lot_rdom.sop_type
					and	lot_rdom.sop_type_recipient in ('P', 'E')
			) innerLink
			) link
		where
			sloc.par_id = sdom.par_id
		and	sloc.loc_id = sdom.loc_id
		and	sdom.par_id = splan.par_id
		and	sdom.dom_id = splan.dom_id
		and	splan.par_id = link.par_id
		and	splan.sop_id = link.presorting_sop_id
		and	splan.sop_type = 'P'
		and	link.par_id = rplan.par_id
		and	link.followup_spl_id = rplan.sop_id
		and	link.followup_sop_type = rplan.sop_type
		and	rplan.par_id = rdom.par_id
		and	rplan.dom_id = rdom.dom_id
		and	rdom.par_id = rloc.par_id
		and	rdom.loc_id = rloc.loc_id
	       	and sloc.name  like '$senderLocation'
	       	and sdom.name  like '$senderDomain'
	       	and splan.name like '$senderPlan'
	       	and rloc.name  like '$receiverLocation'
	       	and rdom.name  like '$receiverDomain'
	       	and rplan.name like '$receiverPlan'
	       	and link.conn_type like '$connType'

	));
}

# ------------------------------------------------------------
$COMMANDS->{"releaseSpace"} =
  {
   alias => undef,
   description => "Show space requirements for 'create release",
   synopsis => 
   [
    [],
	     ["-p", "par_id", 'Source Release Slot'],
   ],
   man=>qq(
	Show the required space (i.e. the space currently used for
	the Source Release Slot) and the available space per Tablespace.
	If required space exceeds the the available space, a 'create release'
	will fail.
), 
   routine => \&releaseSpace
    };


sub releaseSpace {
	my ($this, $argv, $opts) = @_;

	my $parId = $opts->{p};
	unless ($parId) {
	       print "No par_id given.\n";
	       return;
	};
	sql->run(qq(
		SELECT used_space.tablespace_name,
		       TRUNC (bytes_available / (1024 * 1024)) MB_available,
		       TRUNC (bytes_used / (1024 * 1024)) MB_used
		  FROM (SELECT   tablespace_name,
				 SUM (BYTES) bytes_available
			    FROM (SELECT tablespace_name,
					 BYTES
				    FROM DBA_FREE_SPACE ufsp
				  UNION ALL
				  SELECT tablespace_name,
					 maxbytes - BYTES BYTES
				    FROM DBA_DATA_FILES
				   WHERE autoextensible = 'YES')
			GROUP BY tablespace_name) free_space,
		       (SELECT   useg.tablespace_name,
				 SUM (useg.BYTES) bytes_used
			    FROM USER_SEGMENTS useg,
				 (SELECT table_name seg_name,
					 partition_name part_name
				    FROM Release_Tab_Partitions_V relp
				  UNION ALL
				  SELECT uinp.index_name seg_name,
					 uinp.partition_name part_name
				    FROM Release_Tables_V relt,
					 USER_INDEXES uind,
					 USER_IND_PARTITIONS uinp
				   WHERE relt.table_name = uind.table_name
				     AND uind.index_name = uinp.index_name) part_seg
			   WHERE useg.segment_name = part_seg.seg_name
			     AND useg.partition_name = part_seg.part_name
			     AND useg.partition_name LIKE '%x_' || TO_CHAR ($parId) ESCAPE 'x'
			GROUP BY useg.tablespace_name) used_space
		 WHERE used_space.tablespace_name = free_space.tablespace_name
));
}

# ------------------------------------------------------------
$COMMANDS->{"sop"} =
  {
   alias => undef,
   description => "SOP control funtionality",
   synopsis => 
   [
    [],
    ["-s", "par_id", "Show SOP state in that release"],
    ["-S", "par_id", "do from scratch"],
    ["-F", "par_id", "do forced"],
    ["-Q", undef, "Shorten sop job queue (DANGEROUS - for testing) only"]
   ],
   man => qq(Control or show SOP activities. The SOP state tells
	you whether SOP is idle or what it is doing.

	With no parameters, shows runnig SOP, GTM, ASI, REM batch
	jobs by their SIDs),
   routine => \&sop
    };

sub getSopState {
	my ($parId) = @_;
	my $state;
	my $stmt = (qq(begin :state := sop_events.getsopstate($parId); end;));
	my $sth = sql->prepare($stmt);
	$sth->bind_param_inout (":state", \$state, 80);
	$sth->execute();
	print "Slot $parId: $state\n";
	if ($state eq "FROM SCRATCH") {
		my $rwi;
		my $exp;
		my $comp;
		my $prop;
		my $stmt = qq(begin sop_events.getFsWorkloadWeight($parId, :rwi, :exp, :comp, :prop); end;);
		my $sth = sql->prepare($stmt);
		$sth->bind_param_inout (":rwi", \$rwi, 80);
		$sth->bind_param_inout (":exp", \$exp, 80);
		$sth->bind_param_inout (":comp", \$comp, 80);
		$sth->bind_param_inout (":prop", \$prop, 80);
		$sth->execute;
		print ("\t rwi=$rwi, exp=$exp, comp=$comp, prop=$prop\n");
		sql->run(qq(select par_id, location, domain, cmd_name from sop_job_queue_details_v where status = 'PROCESSING'));
		return;
	} else {
		my $sdomExp;
		my $sdomProp;
		my $ddomRwi;
		my $ddomExp;
		my $ddomProp;
		my $stmt = qq(begin sop_events.getBgWorkloadWeight($parId, :sdomExp, :sdomProp, :ddomRwi, :ddomExp, :ddomProp); end;);
		my $sth = sql->prepare($stmt);
		$sth->bind_param_inout (":sdomExp", \$sdomExp, 80);
		$sth->bind_param_inout (":sdomProp", \$sdomProp, 80);
		$sth->bind_param_inout (":ddomRwi", \$ddomRwi, 80);
		$sth->bind_param_inout (":ddomExp", \$ddomExp, 80);
		$sth->bind_param_inout (":ddomProp", \$ddomProp, 80);
		$sth->execute;
		print ("\t sdomExp=$sdomExp, sdomProp=$sdomProp, ddomRwi=$ddomRwi, ddomExp=$ddomExp, ddomProp=$ddomProp\n");
		sql->run(qq(select par_id, location, domain, cmd_name from sop_job_queue_details_v where status = 'PROCESSING'));
		return;
	}


}
sub sop {
	my ($this, $argv, $opts) = @_;

	my $sid = "regexp_replace(regexp_substr(session_id, ',[[:digit:]]*,'),',','')";
	if (my $parId = $opts->{"s"}) {
		getSopState($parId);
		return;
	}

	if (my $parId = $opts->{"S"}) {
		sql->run(qq(begin sop_control.do_from_scratch($parId); end;));
		return;
	}

	if (my $parId = $opts->{"F"}) {
		sql->run(qq(begin sop_control.do_forced($parId); end;));
		return;
	}

	if ($opts->{Q}) {
		sql->run(qq(
                begin
		delete from sop_job_queue 
		  where sjq_id > (select min(sjq_id) from sop_job_queue where status='SCHEDULED') + 10;
		commit;
		end;
		));
		return;
	}

	# with no parameters show running batch jobs
		sql->run(qq(
		    select par_id, 
			decode (max(sop), NULL, '  -  ', to_char(max(sop))) SOP,
			decode (max(gtm), NULL, '  -  ', to_char(max(gtm))) GTM,
			decode (max(rem), NULL, '  -  ', to_char(max(rem))) REM,
			decode (max(asi), NULL, '  -  ', to_char(max(asi))) ASI
			from (
				select par_id, $sid SOP, null GTM, null REM, null ASI from locks_held_v where upper(reason) like '%SOP%'
				union all
				select par_id, null SOP, 1 GTM, null REM, null ASI from locks_held_v where upper(reason) like '%GTM%'
				union all
				select par_id, null SOP, null GTM, 1 REM, null ASI from locks_held_v where upper(reason) like '%REM%'
				union  all
				select par_id, null SOP, null GTM, null REM, 1 ASI from locks_held_v where upper(reason) like '%ASI%'
				union all select par_id,null, null,null,null from release
			) group by par_id
			));
		return;
}


# ------------------------------------------------------------
$COMMANDS->{"gtm"} =
  {
   alias => undef,
   description => "Change all of GTM status",
   synopsis => 
   [
    [],
	     ["-p", "par_id", 'Release Slot'],
	     ["-u", undef, 'Bring everything up-to-date'],
	     ["-o", undef, 'Outdate everything'],
   ],
   routine => \&gtm
    };

sub gtm {
	my ($this, $argv, $opts) = @_;
	my $parId = $opts->{p};
	unless ($parId) {
		print "no par_id given\n";
		return;
	}
	my $ordering;
	my $operation;
	if ($opts->{u}) {
		$ordering = 'DESC';
		$operation = 'brought up-to-date';
	}
	if ($opts->{o}) {
		$ordering = 'ASC';
		$operation = 'outdated';
	}
	unless ($ordering) {
		print "Specify one of -u or -o !\n";
	}

	sql->run(qq(
		begin
			for i in (
				select max(depth), tg.par_id, tg.tar_id
				from
				(
					select
						level depth, par_id, tar_id_dep tar_id
					from
						has_dependency
					start with
						par_id=$parId
					and	tar_id=0
					connect by 
					       prior par_id = par_id
					and    prior tar_id_dep = tar_id
				) hd, target tg
				where
					hd.par_id = tg.par_id
				and	hd.tar_id = tg.tar_id
				and	tg.bcm_id is not null
				group by tg.par_id, tg.tar_id
				order by max(depth) $ordering
			)
			loop
				update target
				set 
					make_seq_num=Seq_Make_Seq_Num.nextval,
					status='S'
				where par_id = i.par_id
				and tar_id = i.tar_id
				;
			end loop;
		end;
	));
	print "Slot $parId has been $operation. You may want to commit now.\n";
}

# ------------------------------------------------------------
$COMMANDS->{changewatcher} =
  {
   description => "Prevent changes on a particular release",
   synopsis => 
   [
    [],
    ["-p","Par_id","watch this release"],
    ["-r",undef,"remove change whatcher"],
   ],
   routine => \&changeWatcher,
   man => qq(This creates triggers on every table with a par_id column. Any attempt to
	intert, update delete on the given par_id raises an exception and the violation is
	logged.

	Setting NLS_TERRITORY to 'ALGERIA' disables the tirgger for the current session.)
  };

sub changeWatcher {
	my ($this, $argv, $opts) = @_;

	my $parId = $opts->{p};
	unless ($parId) {
		print "No par_id given.\n";
		return;
	}

	my $prefix="chw19";

	if ($opts->{r}) {
		my $stmt = qq(select trigger_name from user_triggers where trigger_name like upper('${prefix}$parId%'));
		#print "$stmt\n";
		my $sth = sql->prepare($stmt);
		$sth->execute();
		while (my $row = $sth->fetchrow) {
			sql->run('drop trigger '.$row);
		}

	} else {
		my $stmt = qq(
			select table_name
			from user_tab_columns uc
			where column_name = 'PAR_ID'
			--and table_name = 'SRC_PRODUCT_GROUP'
			and not exists (
				select 1 from user_views uv
				where uv.view_name = uc.table_name
			)
		);
		my $sth = sql->prepare($stmt);
		$sth->execute();
		while ( my $row = $sth->fetchrow) {
			my $triggerName = substr("$ {prefix}${parId}_$row",0,30);
			my $trigger = qq(
				create or replace trigger $triggerName
				before insert or update or delete
				on $row
				for each row
				declare
					op varchar2(32);
					tx varchar2(80);
				begin
					select value
					into tx
					from nls_session_parameters
					where parameter = 'NLS_TERRITORY'
					;
					if tx = 'ALGERIA' then return; end if;

					if :new.par_id = $parId or :old.par_id = $parId then
						case
						when inserting then op:='Inserting into';
						when updating then op:='Updating';
						when deleting then op:='Deleting from';
						end case;

						db_trace.trace(-1,'ChangeWatcher', op, '$row');
						raise_application_error(-20202, op ||' '|| '$row');
					end if;
				end;
			);
			#print "$trigger\n";
			sql->run($trigger);
		}

	}

}


$COMMANDS->{csfProduct} =
  {
   description => "print various information about a CSF Sortplan",
   synopsis => 
   [
    ["filename"],
    ["-N","pname","find product by name"],
    ["-S","sortcode","find product by sortcode"],
    ["-D", undef,"show definition"],
    ["-d", undef,"show matching destination line"],
    ["-A", undef,"show Actions"],
    ["-O", undef,"show Outlets"],
   ],
   routine => \&csfProduct,
   man => qq(Search for a product, either by name or by a contained
	sortcode. By default only the names of the matching products
	are printed. Additional options allow displaying additional
	information about the matching products.
)
  };

sub csfProduct {
	my ($this, $argv, $opts) = @_;

	unless (-r $argv->[0]) {
		print "No or invalid filename\n";
		return;
	}
	unless($opts->{N} xor $opts->{S}) {
		print "Choose either -N or -S\n";
		return;
	}

	my $filename = $argv->[0];
	my $plan = CsfSortplan->new("$filename");

	my @productNames;
	if ($opts->{N}){
		@productNames = ($opts->{N});
	}
	if ($opts->{S}) {
		@productNames = $plan->findSortcode($opts->{S});
	}


	print join ("\n", @productNames)."\n";;
	foreach my $p (@productNames) {
		my $dest = $plan->containsSortcode($p, $opts->{S});
		$plan->printDestination($dest) if $opts->{d};
		$plan->printProductDefinition($p) if $opts->{D};
		$plan->printProductActions($p) if $opts->{A};
		$plan->printProductOgr($p) if $opts->{O};
	}

}


$COMMANDS->{tjournal} =
  {
   description => "access ADM-SPM's journal",
   synopsis => 
   [
    [],
    ["-p","par_id","the release (all)"],
    ["-d","dateTime","yyyy-mm-dd-hh:mm. No spaces! (current time)"],
    ["-r", "timeRange", "range in minutes to consider before/after dateTime (1)"],
    ["-P", "pattern", "pattern to match system or text"]
   ],
   routine => \&tjournal,
man=>qq(Show entries from the journal as they would appear in the JOU
	component.  Language is always english. The time shown will be
	"timeRange" minutes before and after the "dateTime". DateTime
	must be set, timeRange defaults to one minute.

	You can further restrict the results by providing a
	"pattern". Either the system name (GTM, ASI ...) or the
	english text must match this pattern to be shown.  ) };

sub tjournal {
	my ($this, $argv, $opts) = @_;
	my $date = $opts->{d};
	unless ($date) {
		my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
		$date = sprintf("%4d-%02d-%02d-%02d:%02d", $year+1900, $mon+1, $mday, $hour, $min);
		print $date."\n";
	}
	my $parIdClause = $opts->{p} ? "and jl.par_id = $opts->{p}" : "";
	my $timeRange = ($opts->{r} ? $opts->{r}: "1")."/(24*60)";
	my $pattern = "'%".$opts->{P}."%'";

	my $sth = sql->prepare (qq(
		select jl.par_id,
		       jl.insert_timestamp, 
		       jl.host_name,
		       jl.initiator,
		       mml.text, 
		       jl.parameters
		from	emps_journal_logbook jl,
			emps_message msg,
			emps_message_ml mml
		where	jl.insert_timestamp between 
			to_date('$date','yyyy-mm-dd-hh24:mi') - $timeRange and
			to_date('$date','yyyy-mm-dd-hh24:mi') + $timeRange
		$parIdClause
		and	jl.id = msg.message_id
		and	msg.msg_id_ml = mml.msg_id_ml 
		and	mml.iso_abb = 'en'
		and	(upper(jl.parameters) like upper($pattern) or upper(mml.text) like upper($pattern) or upper(jl.system_name) like upper($pattern))
		order by eml_id
	));
	$sth->execute();

	my $fullFormat = "%-6s | %-16s | %-10s | %-10s | %s\n";
	printf ($fullFormat, "par_id", "time", "host", "user", "text");
	for (my $i=0; $i<80; $i++) {print "-";}
	print "\n";

	while (my $row = $sth->fetchrow_arrayref) {
		my $format = $row->[4];
		my $params = $row->[5];

		# make a printf format string
		$format =~ s/%./%s/g;

		# split params and discard fist one (number of params)
		my @params = split('::', $params);
		shift @params;

		my $text = sprintf ("$format", @params);
		printf($fullFormat, $row->[0], $row->[1], $row->[2], $row->[3], $text);
	}
}


$COMMANDS->{mas} =
  {
   description => "show mail attribute sets",
   synopsis => 
   [
    [],
    ["-p","par_id","the release (1)"],
    ["-o","ordering","(1,2)"],
   ],
   routine => \&mas
  };

sub mas {
	my ($this, $argv, $opts) = @_;
	my $parId = $opts->{p} ? $opts->{p} : (sql->runArrayref("select min(par_id) from release"))->[0]->[0];

	my $nCols = (sql->runArrayref("select count (*) from mac where par_id = $parId"))->[0]->[0];
	my $cats = (sql->runArrayref("select mac_id from mac where par_id = $parId order by mac_id"));

	# get the required colums
	my @ord = (1 .. $nCols);
	@ord = split(",", $opts->{o}) if $opts->{o};
	# prefix with 'c'
	my @ordering = map {'c'.$_}  @ord;

	# must order all columns
	if (scalar(@ord) != $nCols) {
		print "Ordering needs to list $nCols values\n";
		return;
	} else {
	       $nCols = scalar(@ord)
	}


	# initial and last (aggregated) column:
	my $allCategories     = join(",",(@ordering[0 ..$nCols-1]));
	my $initialCategories = join(",",(@ordering[0 ..$nCols-2]));
	my $lastCategory = $ordering[$nCols-1];

	#print "ordering=@ordering; nCols=$nCols; all=$allCategories; init=$initialCategories; last=$lastCategory \n";


	sql->run(qq(
select 
       decode (row_number() over (partition by mas_id order by $allCategories) ,
       	      1, mas_code,
	      ''
       ) mas_code,
       $allCategories
from (
	select 
	mas_id, mas_code, $initialCategories, sum_to_csv($lastCategory) $lastCategory
	from
	(
		select 
		       mas.code mas_code,
		       mas.mas_id mas_id,
		       (
		       select mat.code from malc, mat
		       where malc.par_id = masc.par_id
		       and   malc.mal_id = masc.mal_id
		       and   malc.par_id = mat.par_id
		       and   malc.mat_id = mat.mat_id
		       and   mat.mac_id=$cats->[$ord[0]-1]->[0]
		       ) $ordering[0],
		       (
		       select mat.code from malc, mat
		       where malc.par_id = masc.par_id
		       and   malc.mal_id = masc.mal_id
		       and   malc.par_id = mat.par_id
		       and   malc.mat_id = mat.mat_id
		       and   mat.mac_id=$cats->[$ord[1]-1]->[0]
		       ) $ordering[1],
		       (
		       select mat.code from malc, mat
		       where malc.par_id = masc.par_id
		       and   malc.mal_id = masc.mal_id
		       and   malc.par_id = mat.par_id
		       and   malc.mat_id = mat.mat_id
		       and   mat.mac_id=$cats->[$ord[2]-1]->[0]
		       ) $ordering[2]
		from
			mas,
			masc
		where 1=1
		        and	mas.par_id = 1
			and	mas.par_id = masc.par_id
			and	mas.mas_id = masc.mas_id
	--		and	mas.mas_id=63
		order by 1,2,3,4,5
	)
	group by mas_id, mas_code, $initialCategories
) it
order by mas_id, $allCategories
));

}

1;
