
=head1 NAME

Bind - Functions for bind variables

=head1 RESPONSIBILITIES

Provide functions to declare, set, bind and print bind variables. Host
the current settings of bind variables.

=head1 COLLABORATIONS

SqlSession is where the binding actually happens.

=cut

# $Id: Bind.pm,v 1.1 2011/03/25 14:36:23 ekicmust Exp $

package Bind;
use DBI;
use DBD::Oracle;
use ConIo;
use Settings;

use strict;
use vars qw (
	$VARS
	$COMMANDS
	@ISA
);

$VARS={};
@ISA = qw(Plugin);


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
$COMMANDS->{variable} =
  {
   alias => "var",
   description => "declare a bind variable",
   synopsis => [
		["name type"],
		["-s", "value", "set value right away"] ,
		],
   routine => \&variable,
   };


sub variable {
	my ($this, $argv, $opts) = @_;
	my $var = $argv->[0];
	my $type = $argv->[1];
	my $length;

	if ($type =~ /number|^char/i) {
		$length=40;
	} elsif ($type =~ /char/i) {
		# get the length from inside the braces
		$type =~ s/.*\((.*)\).*/$1/;
		$length = $type
	} else {
		print "Cannot declare variables of type $type\n";
		return;
	}
	declareVar(":$var", $length, $opts->{s});
}

$COMMANDS->{print} =
  {
   alias => "p",
   description => "print named or all bind variables",
   synopsis => [
		["name"],
		],
   routine => \&printV,
   };


sub printV {
	my ($this, $argv, $opts) = @_;
	my $var = $argv->[0];

	if ($this->noArgs(@_)) {
		dumpVars();
		return;
	}

	printVar ($var);
}

# ----------------------------------------
sub extractVars {
	my ($stmt) = @_;
	# return an array of the bind variables in stmt

	# get rid of quoted strings
	#$stmt =~ s/'[^\']+'//g;
	$stmt =~ s/'[^\']*'//g;

	# get rid of double quoted strings
	#$stmt =~ s/"[^\"]+"//g;
	$stmt =~ s/"[^\"]*"//g;

	# now get the bind variables
	my @vars;
	while ($stmt =~ s/(:\w+)//) {
		push (@vars, $1)
	}
	return @vars;
	
}
# ----------------------------------------
sub declareVar {
	# declare a variable optionally with length and init value

	my ($var, $length, $value) = @_;
	my $var_conf = $Bind::VARS->{$var} = {};
	$var_conf->{value} = $value;
	$var_conf->{length} = $length ? $length : 40;
	1;
}
# ----------------------------------------
sub bindVar {
	# bind :var for $sth

	my ($sth, $var) = @_;
	my $pValue = \$Bind::VARS->{$var}->{value};
	my $len = $Bind::VARS->{$var}->{length};

	if (!($len > 0)) {
		print "bind: No such variable $var\n";
		return 0;
	}
	return unless $sth;
	$sth->bind_param_inout("$var", $pValue, $len);
	return 1;
}

# ----------------------------------------
sub bindAllVars {
	# bind all variables in statement

	my ($sth, $stmt) = @_;
	# don't bind explain plan statemenets
	if ($stmt =~ /^\s*explain/i) {
		return;
	}
	foreach my $var (extractVars ($stmt)) {
		bindVar ($sth, $var) or return 0;
	}
	return 1;
}

# ----------------------------------------
sub printVar {
	# print the current values of $var
	my ($var) = @_;
	my $value;
	my $len;
	
	# add a colon to $var if not present yet
	$var = ":$var" unless $var =~/^:/;

	if (exists $Bind::VARS->{$var}) {
		$value = $Bind::VARS->{$var}->{value};
		$len = $Bind::VARS->{$var}->{length};
	} else {
		print "print: No such variable $var\n";
		return;
	}


	(my $line = $value) =~ s/./\-/g;
	print "\n".$var."\n";
	print "$line\n";
	print "$value\n";
}

# ----------------------------------------
sub dumpVars {
	# print all variables names, lengths and values;

	print "\nCurrently defined bind variables:\n\n";
	printf ("%-16s %-8s %-55s\n", "var","length","value");
	print "-------------------------------------------------\n";
	foreach my $var (keys(%$Bind::VARS)) {
		my $var_conf = $Bind::VARS->{$var};
		printf ("%-16s %-8s %-55s\n", 
			$var, $var_conf->{length}, $var_conf->{value});
	}
}


Setting::new ($COMMANDS, "verify", "OFF", "ON", "print ampersand replacing");

#----------------------------------------
sub ampersandReplace {
	# replace &variables with values from Settings

	my ($stmt) = @_;
	my $replaced = 0;
	my $old = $stmt;
	my $n=0;

	# iterate untils all "&"s are gone
	while($stmt =~ /&(\w+)/) {
		if ($n++ > 1024) {
			print "Too many iterations in ampersandReplace\n";
			return;
		}
		my $match = $1;
		my $value;

		# we know the value already
		my $value = define->get($match);
		if (!$value) {
			# we don't know the values + must prompt
			conOut "Enter value for $match: ";
			$value =  conIn;
			chomp ($value);
		}
		$stmt =~ s/&$1\.?/$value/g;
		$replaced = 1;
	}
	# optionally show the old and new values 
	if ($replaced && setOn("verify")) {
		conOut "\nold: $old\n";
		conOut  "new: $stmt\n";
	}
	return $stmt;
}

# ----------------------------------------
package Bind_test;

sub test {
	my $dbh =  DBI->connect('dbi:Oracle:C28','isb','isb')
		       or die "Unable to connect: $DBI::errstr";



	my $sth = $dbh->prepare (q(
			create or replace procedure proc_in_out(
				foo in varchar2, bar out varchar2)
			as
				begin
				bar := '999-'||foo;
				end;
			       ));
	$sth->execute;
	print "sql " . $dbh->errstr;
	print "pls " . $dbh->func('plsql_errstr');


	Bind::declareVar(":foo", undef, "theFoo");
	Bind::declareVar(":bar",99, "theBar");

	my $stmt =q{
		       BEGIN
			   PROC_IN_OUT(:foo, :bar);
		       END;
		 };

	$sth = $dbh->prepare($stmt);
	Bind::bindAllVars($sth, $stmt);

	$sth->execute;

	print "\n--------\n";
	Bind::printVar(":foo");
	Bind::printVar(":bar");

	Bind::dumpVars;
}

