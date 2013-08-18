
=head1 NAME

SqlSession - manage an Sql session

=head1 RESPONSIBILITIES

Provide methods similar to the dbh and sth methods. Add Senora-style
error handling. 

=head1 COLLABORATIONS

=cut

# $Id: SqlSession.pm,v 1.2 2013/05/14 10:11:20 draumart Exp $

package SqlSession;
use Feedback;
use Settings;
use Misc;
use FileHandle;
use DBI;
use DBD::Oracle; #qw(:ora_session_modes);;
use Time::HiRes;

use strict;
use vars qw (
	     @ISA
	     @EXPORT
);

# ------------------------------------------------------------
sub new {
# ------------------------------------------------------------
	my ($SqlSession, $params) = @_;
	my $class = ref($SqlSession) || $SqlSession;

	my $this={
		  dbh => undef,
		  sth => undef,
		  user => undef,
		  service => undef,
		  nature => undef,
		  serveroutput => 0
		  };
	bless ($this, $class);

	if (my $cs = $params->{connectString}) {
		$this->connectFromString($cs, $params->{sessionMode}) or return 0;
	}

	return $this;
}
access('nature');
access('serveroutput');


# ----------------------------------------
sub finish {
	my ($this, $statement) = @_;

	return unless $this->{sth};
	$this->{sth}->finish;
	$this->{sth}=undef;
}

# ----------------------------------------
sub canBind {
	# answer if we want to bind variables
	my ($sqlMode, $nature) = @_;

	return 1 if ($sqlMode eq "plsql" && !$nature);
	return 1 if ($sqlMode eq "sql");
	return 1 if ($nature && $sqlMode eq "sql" && $nature->{operation} eq "SELECT");
}
# ----------------------------------------
sub prepare {
	my ($this, $statement, $sqlMode) = @_;

	$statement=~ s/\s*$//m;
	if (setOn("pluginCode")) {
		print "$statement\n";
	}

	return if (setOn("nop"));

	my $nature = Feedback::natureOfStatement($statement);
	
	#my $sth = $this->{dbh}-> prepare ($statement);
	# work around Oracle Bug No.  2548451
	# performing "select column_of_type_Number from
	# synonym_of_a_remote_table_817"
	my $sth = $this->{dbh}-> prepare ($statement, { ora_check_sql => 0 });
	if (!$sth) {
		Feedback::printError;
		return 0;
	} 

	# don't bind :strings inside create package etc.
	if (canBind ($sqlMode, $nature)) {
		Bind::bindAllVars($sth, $statement) or return 0;
	}

	if (!$sth) { 
		$|=1;
		Feedback::printError;
		return 0;
	}
	# remember nature if successful
	$this->{nature} = $nature if $nature;

	return ($this->{sth} = $sth);
}


# ----------------------------------------
sub execute {
	my ($this) = @_;
	return 0 if !$this->{sth};
	return if (setOn("nop"));
	my $rc = undef;

	eval {
	     $rc = $this->{sth}->execute();
	};
	if (!$rc) {
		Feedback::printError;
		return 0;
	}

	# need to comment success of some statements
	if (Feedback::report($this)) {
		return 0;
	}

	return $this->{sth};
}

# ----------------------------------------
sub fetch {
	# raw and crude fetch
	return if (setOn("nop"));
	my ($this) = @_;

	while (my $row = $this->{sth}->fetchrow_arrayref()) {
		print "@$row\n";
	}
	return 1;
}

# ----------------------------------------
sub runArrayref {
  	# run command and fill array
  	my ($this, $statement, $array) = @_;
	return if (setOn("nop"));
	my $result = $array ? $array : [];

	my $sth = $this->prepare($statement) or return;
	$sth->execute();
	while (my $row = $sth->fetchrow_arrayref) {
		# must copy arrayref
		my $rowCopy = [];
		@$rowCopy = @$row;
		push (@$result, $rowCopy);
	}
	return $result;
}

# ----------------------------------------
sub milliseconds {
	my ($this) = @_;
	my ($sec, $usec) = Time::HiRes::gettimeofday();
	return int(($sec*1000 + $usec/1000) + 0.5);
}

# ----------------------------------------
sub run {
	# prepare + execure + fetch
	my ($this, $statement, $sqlMode) = @_;
	my $tstart = $this->milliseconds();

	#print "Statement6=$statement";
	my $sth = $this->prepare($statement, $sqlMode) or return;
	return if (setOn("nop"));

	$this->execute() or return;
	Feedback::printError();
	# some statements do not require a fetch.
	if ($sth->{NUM_OF_FIELDS} == 0) {
		Feedback::printError();
		my $texec = $this->milliseconds();
		printf ("Elapsed: total= %0.3f sec\n",
		($texec-$tstart)/1000) if (setOn("feedback"));

		return 1;
	}
	my $texec = $this->milliseconds();
	my $lastRow = $this->fetch;
	my $tfetch = $this->milliseconds();
	printf ("Elapsed: prepare+execute = %0.3f sec; fetch= %0.3f sec; total= %0.3f sec\n",
		($texec-$tstart)/1000, ($tfetch-$texec)/1000, ($tfetch-$tstart)/1000) if (setOn("feedback"));
	return $lastRow;
}
# ----------------------------------------
sub connectFromString {
	# connect with a classic/connect@string
	my ($this, $connectString, $sessionMode) = @_;
	my $mode;

	my ($userpass,$service)= split ("[@]", $connectString);
	my ($user,$pass)= split ("[/]", $userpass);

	#mode = 2;	# SYSDBA
	#mode = 4;	# SYSOPER

	if ($sessionMode =~ /sysdba/i) {
		$mode = 2;
	}
	if ($sessionMode =~ /sysoper/i) {
		$mode = 4;
	}

	my $dbh=$this->connect($user, $pass, $service, $mode);
	print "$DBI::errstr\n" unless $dbh;
	$dbh->{RowCacheSize} = 64 if $dbh;
	return $dbh;
}

# ----------------------------------------
sub promptForPassword {
	my ($this, $user, $service) = @_;
	my $pass;

	unless (-t) {
		print "Not connected to a terminal. Cannot prompt for password\n";
		return;
	}

	require Term::ReadKey;
	Term::ReadKey::ReadMode('noecho');
	print STDERR "Password for $user" . ($service ? " on $service: " : ": ");
	chomp ($pass = Term::ReadKey::ReadLine(0));
	Term::ReadKey::ReadMode('restore');

	return $pass;
}

# ----------------------------------------
sub connect {
	my ($this, $user, $pass, $service, $mode) = @_;

	$pass = $this->promptForPassword($user, $service) unless $pass;

	unless ($user) {
		print "no user";
		return;
	}
	unless ($pass) {
		print "no password";
		return;
	}

	my $handle = DBI->connect
	  (
	   "dbi:Oracle:$service", 
	   "$user", "$pass",
	   { RaiseError => 0, 
	     PrintError => 0, 
	     AutoCommit => 0, 
	     ShowErrorStatement  => 0,
	     ora_session_mode => $mode}
	   ) ;

	if ($handle) {
		$this->{dbh} = $handle;
		$this->{dbh}->{LongReadLen}=64000;
		$this->{user} = $user;
		$this->{service} = $service;
		return $handle; 
	} else {
		#print "can't connect.\n";
		return 0;
	}

}

# ----------------------------------------
sub disconnect {
	my ($this) = @_;
	$this->{dbh} -> disconnect;
}

# ----------------------------------------
sub identify {
	# return a string describing current connection

	my ($this) = @_;
	return "$this->{user}\@$this->{service}";
}

# ----------------------------------------
sub user {
	# return a string describing current user

	my ($this) = @_;
	return "$this->{user}";
}

# ----------------------------------------
sub service {
	# return a string describing current instance

	my ($this) = @_;
	return "$this->{service}";
}

1;
