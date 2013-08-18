=head1 NAME

SessionMgr - Plugin to manage multiple Senora session

=head1  RESPONSIBILITIES 

Provide commands to connect, disconnect and swicth between
sessions.

=head1  COLLABORATIONS

Uses Sql_Formatter to actually connect/disconnect

=cut

# $Id: SessionMgr.pm,v 1.1 2011/03/25 14:36:24 ekicmust Exp $

package SessionMgr;
use Exporter;
use Misc;
use Settings;
use DBI;
use DBD::Oracle;
use strict;
use vars qw (
	     @ISA
	     $COMMANDS
	     @EXPORT
	     @EXPORT_OK
);

@ISA = qw (
	   Plugin
	   Exporter
);

access ('mainSession');
@EXPORT=qw(sql connected);
@EXPORT_OK=qw(sessionNr sessionMain);

# ------------------------------------------------------------
sub new {
	my ($SessionMgr, $params) = @_;
	my $class = ref($SessionMgr) || $SessionMgr;

	my $this = {};
	$this->{sessions} = [];
	$this->{mainSession} = undef;;

	$this->{commands} = $COMMANDS;

	bless ($this, $class);

	return $this;
}


# ------------------------------------------------------------
$COMMANDS->{oracle_version} =
  {
   alias => "orav",
   description => "Print oracle version information",
   routine => \&oracle_version,
   };

sub oracle_version {
	my($this, $argv, $opt) = @_;
	my $sql = $Senora::SESSION_MGR->{mainSession};
	welcome2($this, $sql, 100);
}

sub welcome2 {
	# print login message 
	my ($this, $dbh, $rows) = @_;

	$rows = 3 unless $rows;

	print "\nConnected to:\n";
	my $sth = $dbh->prepare (
	  qq(
	   select banner from v\$version
	   where rownum < $rows
	   ));
	$sth->execute;
	while (my $line = $sth->fetchrow_arrayref) {
		print "  $line->[0]\n";
	}
}

# ------------------------------------------------------------
sub add_session {
	my ($this, $session) = @_;
	push(@{$this->{sessions}}, $session);
}

# ------------------------------------------------------------
sub last_session {
	my ($this, $session) = @_;
	return $#{$this->{sessions}}
}

# ------------------------------------------------------------
sub all_sessions {
	my ($this, $session) = @_;
	return (@{$this->{sessions}});
}

# ------------------------------------------------------------
sub setPrompt {
	my ($this, $sno) = @_;
	$Senora::PROMPT = $sno .':'.$this->{sessions}->[$sno]->identify ."> ";
}

# ----------------------------------------
$COMMANDS->{connect} =
  {
   alias => "senora|conn|c",
   description => "create new/additional session or list sessions",
   synopsis => 
   [
    ["connectString"],
    ["-a", undef, "add a session (don't diconnect)"],
    ["-d", undef, "disconnect other sessions"],
    ["-s", "sessNo", "switch to given session number"],
    ["-p", "pattern", "switch to session matching pattern"],
    ["-q", undef, "be a bit more quiet"],
    ["-Q", undef, "be really quiet"],
    ],
   man => q(
	Connect create a senora session. You can have multiple
	sessions at a time, which may save you some time, when you
	need to switch sessions frequently.

	With no arguments connect prints all connections.

	The -a and -d options override the default behavior which 
	can be set via "set disconnect".
   ),
   routine => \&connect,
   };

sub connect {
	# create a new session. Optionally preserve old sessions

	my($this, $argv, $opts) = @_;

	if ($this->noArgs(@_)){
		$this->list;
		return;
	}

	# must use defined because value can be zero
	if (defined ($opts->{"s"})) {
		$this->switch($opts->{"s"});
		return;
	}

	if (defined ($opts->{"p"})) {
		$this->switchPattern($opts->{"p"});
		return;
	}
	# decide wether to disconnect existing sessions
	my $disconnect = setOn("disconnect");
	$disconnect = 1 if $opts->{"d"};
	$disconnect = 0 if $opts->{"a"};
	$this->disconnectAll if $disconnect;

	# strip password for display
	my $connection = $argv->[0];
	$connection =~ s/\/\w*//;
	print "Connecting to [$connection] at ".localtime(time) ." ... \n" unless $opts->{Q};
	my $session = SqlFormatter->new (
					 {connectString => $argv->[0],
					 sessionMode => $argv->[2]},
					);

	if ($session) {
		$this->add_session ($session);
		$this->welcome2($session->{dbh}) unless ($opts->{"q"} or $opts->{"Q"});
		$this->{mainSession} = $session;
		$this->setPrompt ($this->last_session);
	} else {
		print "Can't connect.\n";
		Feedback::handleWheneverSqlerror();
	}
}

# ----------------------------------------
$COMMANDS->{disconnect} =
  {
   alias => "dis",
   description => "disconnect session nr",
   synopsis => 
   [
    ["sessionNr"]
    ],
   routine => \&disconnect,
   };

sub disconnect {
	my($this, $argv, $opt) = @_;

	my @sessions;

	# disconnect and mark session as deleted
	foreach my $s (@$argv) {
		my $thisSession = $this->{sessions}->[$s];
		return unless $thisSession;
		$thisSession->disconnect;
		$this->{sessions}->[$s] = 0;

		# desconnected Main Session ?
		if ($thisSession eq $this->{mainSession}) {
			$this->{mainSession} = 0;
			$Senora::PROMPT = $Senora::DEFAULT_PROMPT;
		}

	}

	# cleanup
	my @remainingSessions = ();
	foreach my $ss ($this->all_sessions) {
		push (@remainingSessions, $ss) if $ss;
	}
	$this->{sessions}=\@remainingSessions;

}

# ----------------------------------------
sub disconnectAll {
	my($this, $argv, $opt) = @_;

	#print "disconnecting all\n";
	foreach my $ss ($this->all_sessions) {
		$ss->disconnect;
	}
	$this->{sessions}=[];
	$Senora::PROMPT = $Senora::DEFAULT_PROMPT;

}

# ----------------------------------------
sub list {
	my($this) = @_;
	my $sessno = 0;
	foreach my $ss ($this->all_sessions) {
		print "$sessno\t" . $ss->identify . "\n";
		$sessno++;
	}
}

# ----------------------------------------
sub findSessionNumbered {
	# get session by number
	my($this, $sessionNr) = @_;

	my $session;
	if ($sessionNr =~ /[0-9]+/) {
		$session = $this->{sessions}->[$sessionNr];
	}
	unless ($session) {
		print "No such session.\n";
		Feedback::handleWheneverSqlerror();
		return 0;
	}
	return $session;
}

# ----------------------------------------
sub sessionNr{
	# get session by number (exported)
	my($sessionNr) = @_;
	return $Senora::SESSION_MGR->findSessionNumbered($sessionNr);
}

# ----------------------------------------
sub sessionMain{
	# get session by number (exported)
	my($sessionNr) = @_;
	return $Senora::SESSION_MGR->mainSession;
}

# ----------------------------------------
sub findSessionNamed {
	# find session by pattern
	my($this, $pattern) = @_;

	my $session;
	my $matchLength = 0;
	my $bestMatch;
	for (my $sessionNr=0; $sessionNr<100; $sessionNr++) {
		$session = $this->{sessions}->[$sessionNr];
		if (
		    $session 
		    and $session->identify() =~ $pattern 
		    and length($&) > $matchLength) {
			$bestMatch=$session;
			$matchLength = length($&);
		}
		if (!$session) {
			if ($matchLength > 0) {
				return $bestMatch;
			}
			print "No such session.\n";
			Feedback::handleWheneverSqlerror();
			return 0;
		}
	}
	print "Error in findSessionNamed\n";
	return undef;
}

# ----------------------------------------
sub getSessionNumber {
	# return the sessionNumber of a given session
	my($this, $aSession) = @_;

	for (my $sessionNr=0; $sessionNr<100; $sessionNr++) {
		my $session = $this->{sessions}->[$sessionNr];
		if ($session == $aSession) {
			return $sessionNr;
		}
		if (!$session) {
			Feedback::handleWheneverSqlerror();
			print "No such session.\n";
			return 0;
		}
	}
	print "Error in getSessionNumber\n";
	return undef;
}

# ----------------------------------------
sub switch {
	# make session Nr the main session
	my($this, $sessionNr) = @_;

	if (my $session = $this->findSessionNumbered($sessionNr)) {
		$this->{mainSession} = $session;
		$this->setPrompt ($sessionNr);
	}
}

# ----------------------------------------
sub switchPattern {
	# make session the main session
	my($this, $pattern) = @_;

	my $session = $this->findSessionNamed($pattern) or return;

	$this->{mainSession} = $session;
	$this->setPrompt ($this->getSessionNumber($session));
}

# ----------------------------------------
$COMMANDS->{quit} =
  {
   alias => "exit|q",
   description => "exit Senora.",
   synopsis => 
   [
    ],
   routine => \&quit,
   };

sub quit {
	my($this, $argv, $opt) = @_;

	print "Bye.\n";
	$this->disconnectAll;
	exit;
}

# ------------------------------------------------------------
sub connected {
	# return 1 if there is a main session, print msg if param
	my $s = $Senora::SESSION_MGR->{mainSession};
	return 1 if $s;
	print "Not connected\n" if $_[0];
	return 0;
}

# ------------------------------------------------------------
sub sql {
	# return the main session or $this
	my $s = $Senora::SESSION_MGR->{mainSession};
	return $s if $s;

	# No session: return $this so the dummy routines in here are
	# called. This takes care of some cases where the caller did
	# not bother to check for a connection.
	return $Senora::SESSION_MGR;
}


# ------------------------------------------------------------
sub dummy {
	print "Not connected.\n";
	return $_[0];
}

sub run { return dummy(@_);}
sub prepare { return dummy(@_);}
sub execute { return dummy(@_);}
sub fetchrow_arrayref { return 0;}
sub runArrayref { print "Not connected.\n"; return 0;}

# ----------------------------------------
$COMMANDS->{debugDbi} =
  {
   alias => "",
   description => "set trace level for DBI calls",
   synopsis => 
   [
    [],
    ["-l", "level", "the trace level"],
    ],
   routine => \&debugDbi,
   };

sub debugDbi {
	my($this, $argv, $opt) = @_;

	DBI->trace($opt->{l});
}

newSetting("disconnect","TRUE","FALSE",
"Specify if 'connect' disconnects first");


1;
