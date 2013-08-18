
=head1 NAME

History - manage command history

=head1 RESPONSIBILITIES

Provide commands to add Commands to the history and to rerun them
Remember last statement for comments (foo created).
Remember last sql statement for show errors and list etc.

=head1 COLLABORATIONS

History puts Statement Objects into a Stack object.

=cut

# $Id: History.pm,v 1.1 2011/03/25 14:36:23 ekicmust Exp $

package History;
use Misc;
use Statement;
use strict;
use vars qw 
(
 @ISA
 $COMMANDS
);

@ISA = qw (
	   Plugin
	   Stack
);

# ------------------------------------------------------------
sub new {
	my ($Input, $pluginMgr) = @_;
	my $class = ref($Input) || $Input;
	my $this = Stack->new(128);
	# remember the last reas sql statement
	$this->{lastSql} = undef;
	$this->{current} = undef;
	$this->{entryNo}=1;
	bless ($this, $class);

	$this->{commands} = $COMMANDS;

	return $this;
}

access ('lastSql');
access ('current');



# ----------------------------------------
sub add {
	# Add a statement to the history

	my ($this, $stmt) = @_;
	return if length($stmt)>1000;
	$this->{current} = $stmt;
	$this->push ({
		     entryNo => $this->{entryNo}++,
		     statement => $stmt
		     });
	# always remember last sql statement (for save, list);
	if (
	    $stmt->sqlMode eq "sql" or
	    $stmt->sqlMode eq "plsql" 
	    ) {
		$this->{lastSql} = $stmt;
	}
	    
}


# ----------------------------------------
$COMMANDS->{history} =
  {
   alias => 'hi',
   description => "show history items matching pattern (or all)",
   synopsis => [
		["pattern"]
		],
   routine => \&history,
   };


sub history {
	# show history

	my ($this, $argv, $opts) = @_;
	my $pattern = $argv->[0] ? $argv->[0] : ".";

	# watchout: peek(1) ist the last entry peek(2) the one before
	for (my $i = $this->size; $i>1; $i--) {
		my $entry = $this->peek($i);
		my $stmt = $entry->{statement}->get;
		next unless $stmt =~ /$pattern/;

		print "------------------------[$entry->{entryNo}]\n";
		$stmt =~ s/^\n*//;
		$stmt =~ s/\n*$//;
		print substr($stmt,0,256)."\n";
	}
}

# ----------------------------------------
$COMMANDS->{again} =
  {
   alias => '!|/',
   description => "rerun latest matching entry",
   synopsis => [
		["pattern"]
		],
   routine => \&again,
   };


sub again {
	my ($this, $argv, $opts) = @_;
	my $pattern = $argv->[0] ? $argv->[0] : ".";

	# watchout: peek(1) ist the last entry peek(2) the one before
	for (my $i=2; $i <= $this->size; $i++) {
		my $entry = $this->peek($i);
		my $s = $entry->{statement};
		my $stmt = $s->get;
		next unless $stmt =~ /^$pattern/;
		print "$stmt\n";

		$s->run;
		return;
	}
	print "Senora: !$pattern event not found\n";
}

# ----------------------------------------
$COMMANDS->{rerun} =
  {
   alias => '!!',
   description => "run history item nr",
   synopsis => [
		["nr"]
		],
   routine => \&rerun,
   };


sub rerun {
	my ($this, $argv, $opts) = @_;
	my $nr = $argv->[0];

	if (my $stmt = $this->getItemNumbered($nr)) {
		$stmt->run;
	}
}

# ----------------------------------------
sub getItemNumbered {
	my ($this, $nr) = @_;

	# watchout: peek(1) ist the last entry peek(2) the one before
	for (my $i = $this->size; $i>1; $i--) {
		my $entry = $this->peek($i);
		if ($entry->{entryNo} == $nr) {
			$entry->{statement}->{rline}=0;
			return $entry->{statement};
		}
	}
	print "cannot find history item $nr \n";
}




1;
