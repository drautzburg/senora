# ------------------------------------------------------
# NAME
#	Tuning - Provide commands for BDM
# RESPONSIBILITIES
#
# COLLABORATIONS
#
# History
# ------------------------------------------------------
# $log$
# ------------------------------------------------------

=head1 NAME

Tuning - Provide commands for BDM

=head1 RESPONSIBILITIES

=head1 COLLABORATIONS

=cut

# $Id: Bdm.pm,v 1.1 2010/12/22 14:14:05 ekicmust Exp $

package Bdm;
use strict;
use SessionMgr qw(sessionNr sql);
use Plugin;
use Settings;
use Text::Wrap qw(wrap $columns $huge);
use ConIo;
use Misc;

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
$COMMANDS->{"bdmTables"} =
  {
	  alias => "",
	  description => "Print rowcount of tables",
	  synopsis => 
	    [
	     [],
	     ["-s", "schema", "The source schema (pls_source)"],
	     ["-t", "schema", "The target schema (pls_target)"],
	     ["-m", undef, "Include ML tables"],
	     ["-a", undef, "List individual tables"],
	     ["-d", undef, "Analyze row differences"],
	     ],
	      routine => \&bdmTables,
    };

sub count {
	my ($table) = @_;
	my $res = sql->runArrayref("select count(*) from $table");
	return $res->[0][0];
}

sub diff {
	my ($source, $target) = @_;
	my $res = sql->runArrayref(qq(
			select count(*) from (
				select * from $source
				minus select * from $target
			)
			));
	return $res->[0][0];
}

sub fullDiff {
	my ($source, $target) = @_;
	my $missing = diff($source, $target);
	my $extra = diff ($target, $source);
	my $stotal = count($source);
	my $ttotal = count($target);
	return($missing, $extra, 100 - 100*($missing+$extra)/($stotal+$ttotal));
}

sub bdmTables {
	my ($this, $argv, $opts) = @_;
	my $sourceSchema = $opts->{s} ? $opts->{s} : 'PLS_SOURCE';
	my $targetSchema = $opts->{t} ? $opts->{t} : 'PLS_TARGET';
	my $mlClause = $opts->{m} ? "" : "and table_name not like '%_ML'";
	my $populated = 0;
	my $unpopulated = 0;
	my $completeness = 0;

	my $tables = sql->runArrayref (qq(
			select table_name
			from all_tables
			where owner = '$targetSchema'
			$mlClause
	));

	foreach my $i  (@$tables) {
		my $t = $i->[0];
		my $rows = count("$targetSchema.$t");
		if ($rows > 0) {
			$populated++;
			my ($missing, $extra, $pct) = fullDiff("$sourceSchema.$t", "$targetSchema.$t");
			
			printf("%-32s missing: %d, extra: %d, %3.0f%% ok\n", $t,$missing, $extra, $pct);
			$completeness += $pct;
		} else {
			$unpopulated++
		}
		printf ("%-32s %d\n", $t, count("$targetSchema.$t")) if $opts->{v};
	}
	printf ("%3d Tables populated\n%3d Tables still empty\n%3.0f%% complete\n",
		$populated, $unpopulated, $completeness/($populated+$unpopulated));

}
1;





