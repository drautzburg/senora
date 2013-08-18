# --------------------------------------------------------------------------------  
# NAME
#	SpmIrl - Not needed unless you work on the Spm project
# RESPONSIBILITIES
#
# COLLABORATIONS
#
# History
# --------------------------------------------------------------------------------  
# $log$
# --------------------------------------------------------------------------------  

package Spm;
use strict;
use Plugin;
use SessionMgr;
use Text::Wrap;
use Getopt::Long;
use File::Basename;
use Misc;

Getopt::Long::Configure("no_ignore_case");
Getopt::Long::Configure("bundling");


use vars qw 
(
 @ISA
 $COMMANDS
 $MRM_PATTERNS
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
$COMMANDS->{geolookup} =
  {
   alias => "",
   description => "find a name in geo (requires tra_f_get_item_info)",
   synopsis => 
   [
    ["pattern (user %!)"],
    ["-K","keyColumn","the key column in the geo table"],
    ["-N","nameColumn","the name column in the geo table"],
    ["-T","geoTable","the name of the geo table"],
   ],
   routine => \&geolookup
  };

sub geolookup {
	my ($this, $argv, $opts) = @_;

	my $sql = $Senora::SESSION_MGR->{mainSession};

	my $nameColumn = $opts->{N};
	my $keyColumn = $opts->{K} ;
	my $geoTable = $opts->{T};
	my $pattern = "'".uc($argv->[0])."'";
	if (!$nameColumn) {
		print "no name colum\n";
		return;
	}
	if (!$keyColumn) {
		print "no key colum\n";
		return;
	}
	if (!$geoTable) {
		print "no geoTable\n";
		return;
	}
	if (!$pattern) {
		print "no pattern\n";
		return;
	}

	my $adm = $sql->prepare (qq(
		select han.aic_id, han.ait_id, sop_sortcode, tra_f_get_item_info(han.aic_id, han.ait_id)
			from han, sop
			where han_name like $pattern
			and han.ait_id=sop.ait_id
			and han.aic_id = sop.aic_id
	));
	$sql->execute;

	while (my $row = $adm->fetchrow_arrayref) {
		print "\naic=$row->[0] ait=$row->[1] sortcode=$row->[2]\n";
		my @path=split(",", $row->[3]);
		foreach my $i(@path) {
			$i=~s/^ *//;
			$i=~s/ /\t/;
			print "$i\n";
		}
		print "\n*** src_id lookup yields:";
		$sql->run (qq(
		select 
			'$geoTable' geo_Table,
			$geoTable.*
		from
			ait,
			$geoTable
		where	ait.ait_id = $row->[1]
		and	ait.aic_id = $row->[0]
		and	ait.src_id_1 = $geoTable.$keyColumn
		));
	}
	print "\n*** bare geo table lookup yields:";
	$sql->run(qq(
		select 
			'$geoTable' geo_Table,
			post_town_id, 
			$nameColumn,
			err_code
		from
			$geoTable
		where	$nameColumn like $pattern
	));


}

# ------------------------------------------------------------
$COMMANDS->{geospeed} =
  {
   alias => "",
   description => "count lines in adm_tb_log",
   synopsis => 
   [
    [""],
    ["-T","table_name","log table name (adm_tb_log)"],
    ["-n","line_no","first line no to look at"],
    ["-N","line_no","last line no to look at"]
   ],
   routine => \&geolookup
  };

sub geospeed {
	my ($this, $argv, $opts) = @_;

	my $sql = $Senora::SESSION_MGR->{mainSession};

	my $firstLineNo = $opts->{n} ? $opts->{n} : 0;
	my $lastLineNo = $opts->{N} ? $opts->{N} : 1000000000;

	my $tableName = $opts->{T} ? $opts->{T} : "adm_tb_log";
	my $sth=$sql->prepare(qq(
		select min(line_no),
		to_char(time_stamp,'JSSSSS') seconds,
		to_char(time_stamp,'dd.mm. hh24:mi') time,
		count(*)
		from $tableName
		where 1=1
		and line_no between $firstLineNo and $lastLineNo
		group by 
			to_char(time_stamp,'JSSSSS'),
			to_char(time_stamp,'dd.mm. hh24:mi')
	));
	$sth->execute;
	my $lastt=0;
	my $lastT=0;
	my $total=0;
	printf ("%-12s %-12s %-12s %-12s %s\n", 
		"time",
		"line",
		"total",
		"elapsed",
		"speed");
	while (my $row=$sth->fetchrow_arrayref) {
		my $elapsed = 1+$row->[1]-$lastT;
		printf ("%-12s %-12s %-12s %-12s %4.0f", 
			$row->[2], 
			$row->[0], 
			$total,
			$elapsed,
			$row->[3]/(1+$row->[1]-$lastt)
		       );
		$lastt=$row->[1];

		if ($elapsed > 600) {
			printf "   $total %2.0f\n",$total/($elapsed);
			$total=0;
			$lastT=$lastt;
		}else {
			$total += $row->[3];
			print "\n";
		}
	}
}
1;

