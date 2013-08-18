# ------------------------------------------------------
# NAME
#	Stats - Provide commands for Stats
# RESPONSIBILITIES
#
# COLLABORATIONS
#
# History
# ------------------------------------------------------
# $log$
# ------------------------------------------------------

=head1 NAME

Compare - Provide commands for DBMS_STATS

=head1 RESPONSIBILITIES

=head1 COLLABORATIONS

=cut

# $Id: Stats.pm,v 1.1 2010/12/22 14:14:05 ekicmust Exp $

package Stats;
use Misc;
use strict;
use Plugin;
use SessionMgr qw(sessionNr sessionMain sql);

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



$COMMANDS->{"statTable"} =
  {
	  alias => undef,
	  description => "xxxx",
	  synopsis => 
	    [
	     ["table"],
	     ["-T", "Stattab", "The stattab tables to use (default=data dictionary)"],
	     ["-I", "StatId", "The statId within that table)"],
	     ],
	      routine => \&statTable,
    };

sub statTable {
	my ($this, $argv, $opts) = @_;
	my $stattab = $opts->{T};
	my $statid =  $opts->{I};

	if ($stattab && ! $statid) {
		$statid = 'SENORA';
	}
	if ($statid && !$stattab) {
		$stattab = 'STATTAB';
	}
	if ($stattab) {
		$stattab = "'".uc($stattab)."'";
	} else {
		$stattab = "NULL";
	}

	if ($statid) {
		$statid = "'".uc($statid)."'";
	} else {
		$statid = "NULL";
	}


	my $table = "'".uc($argv->[0])."'";

	my $stmt = qq(
	  begin
	    dbms_stats.get_table_stats (
					ownname => user,
					tabname => $table,
					stattab => $stattab,
					statid => $statid,
					numrows => :numrows,
					numblks => :numblks,
					avgrlen => :avgrlen
					);
	end;);
	my $sth = sql->prepare($stmt);
	print ("error\n") unless($sth);
#	$sth->bind_param(":tabname", $table);
	my $numrows;
	my $numblks;
	my $avgrlen;
	$sth->bind_param_inout(":numrows", \$numrows, 80);
	$sth->bind_param_inout(":numblks", \$numblks, 80);
	$sth->bind_param_inout(":avgrlen", \$avgrlen, 80);
	$sth->execute();

	print "$table $numrows $numblks $avgrlen\n";
}


1;
