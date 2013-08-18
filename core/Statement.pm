
=head1 NAME

Statement - manage a multiline statement

=head1 RESPONSIBILITIES

Provide methods to add lines and annotations to a statememt. Provide
methods to reread a statement.

=head1 COLLABORATIONS

History depends on Statement

=cut

# $Id: Statement.pm,v 1.1 2011/03/25 14:36:25 ekicmust Exp $

package Statement;
use FileHandle;
use SessionMgr;
use strict;
use Misc;
use ConIo;
use vars qw (
	     @ISA
	     $STARTER
	     $COMMANDS
);

@ISA = qw (
);

$STARTER = {
	    sql =>
		'ALTER'
		.'|ANALYZE'
		.'|ASSOCIATE'
		.'|AUDIT'
		.'|CALL'
		.'|COMMENT'
		.'|COMMIT'
		.'|CREATE'
		.'|DELETE'
		.'|DISASSOCIATE'
		.'|DROP'
		.'|EXPLAIN'
		.'|GRANT'
		.'|INSERT'
		.'|LOCK'
		.'|NOAUDIT'
		.'|RENAME'
		.'|REVOKE'
		.'|ROLLBACK'
		.'|SAVEPOINT'
		.'|SELECT'
		.'|WITH'
		.'|TRUNCATE'
		.'|UPDATE'
	        .'|PURGE'
	        .'|MERGE'
	    ,

	    plsql => 
	     'create(\s+or\s+replace)*\s+procedure'
	    .'|create(\s+or\s+replace)*\s+function'
	    .'|create(\s+or\s+replace)*\s+package'
	    .'|create(\s+or\s+replace)*\s+trigger'
	    .'|create(\s+or\s+replace)*\s+type'
	    .'|declare'
	    .'|begin'
	    .'|\/\*plsql\*\/',,
};

access ('sqlMode');

# ------------------------------------------------------------
sub new {
	my ($Statement, $pluginMgr) = @_;
	my $class = ref($Statement) || $Statement;
	my $this={
		  pm => $pluginMgr,
		  statement => [],
		  rline => 0,
		  wline => 0,
		  sqlMode => undef,
		  };
	bless ($this, $class);
	return $this;
}

# ----------------------------------------
sub clear {
	# wipe everything

	my ($this) = @_;
	$this->{statement} = [];
	$this->{lineno} = 0;
}

#----------------------------------------
sub run {
	my ($this) = @_;
	# Call right object to run Statement
	if ($this->sqlMode eq "builtin") {
		my $statement = Bind::ampersandReplace($this->get);
		$Senora::PLUGIN_MGR->runCommand($statement);
	} elsif ($this->sqlMode =~ /sql/) {
		my $statement = Bind::ampersandReplace($this->get);
		sql->run($statement, $this->sqlMode);
	} else {
		print "Hun ?\n";
	}
}

#----------------------------------------
sub setMode {
	# Distinguish between builtin, sql, plsql and perl

	my ($this, $line) = @_;

	# if a plugin can run it is a "bultin"
	if ($Senora::PLUGIN_MGR->canRun($line)) {
		return ($this->{sqlMode} = "builtin");
	}

	if ($line =~ /^\s*($STARTER->{plsql})/i) {
		return ($this->{sqlMode} = "plsql");
	}
	if ($line =~ /^\s*($STARTER->{sql})/i) {
		return ($this->{sqlMode} = "sql");
	}
	chomp($line);
	print "what ? \"$line\" ?\n";
	return undef;
}


# ----------------------------------------

sub get {
	# Assemble the lines in $this->{statement} to a single string
	my ($this) = @_;

	my $statement = $this->{statement};
	return join ("", @$statement);
}

# ----------------------------------------
sub save {
	# save the statement to a file

	my ($this, $filename) = @_;
	my $fd = FileHandle->new(">$filename");
	unless ($fd) {
		print "Cannot open $filename.\n";
		return;
	}
	print $fd $this->get;
	print $fd "\n/\n";
	$fd->close;
}
# ----------------------------------------
sub list {
	# List statement with linenumbers
	my ($this) = @_;

	my $n=0;

	while(my $line = $this->{statement}->[$n]) {
		$n++;
		print "$n  $line";
	}
	print "\n";
}

# ----------------------------------------
sub addline {
	# add a line to $this->{statement}
	my ($this, $line) = @_;

	$this->{statement}->[$this->{wline}] = $line;
	$this->{wline}++;

	# if mode is still unknown set it now
	if ($this->{wline} < 6) {
		my $startOfStatement = $this->get();
		$startOfStatement =~ s/\n/ /g;
		$this->setMode($startOfStatement);
	}
}

# ----------------------------------------
sub readOpen {
	# initiate a read
	my ($this, $line) = @_;

	$this->{rline} = 0;
}

# ----------------------------------------
sub eof {
	# return true if no more lines to read
	my ($this, $line) = @_;

	return ($this->{rline} >= $this->{wline});
}

# ----------------------------------------
sub getline_obsolete {
	# read a line from statement
	my ($this, $line) = @_;

	$this->{rline}++;
	$this->{statement}->[$this->{rline}-1];
}

# ----------------------------------------
sub matches {
	# return true if statement matches string

	my ($this, $string) = @_;
	return ($this->get() =~ /$string/);
}

# ----------------------------------------
sub p_lastLine {
	# return reference to last line
	my ($this) = @_;

	return \$this->{statement}->[$this->{wline}-1];
}
# ----------------------------------------
sub chopSemicolon {
	# remove trailing semicolon of last line
	my ($this) = @_;
	${$this->p_lastLine} =~ s/;\s*$//;
}

# ----------------------------------------
sub removeTerminator {
	# remove lonely slash or semicolon
	my ($this) = @_;

#	if ( ${$this->p_lastLine} =~ /^[\/\;]\s$/ ){
	if ( ${$this->p_lastLine} =~ /^[\/]\s*$/ ){
		${$this->p_lastLine} = undef;
		$this->{wline}--;
		return 1;
	}
	return 0;
}

# ----------------------------------------
sub endOfStatement {
	# Return 1 If line ends statement
	my ($this) = @_;

	my $line = ${$this->p_lastLine};

	#print "Mode=$this->{sqlMode} \nLine=$line\n";
	if ($this->{sqlMode} eq "builtin") {
		# builtins are single line
		$this->chopSemicolon;
		return 1;
	}
	# a lonely slash ends every statement but is not part of it
	$this->removeTerminator && return 1;

	if ($this->{sqlMode} eq "sql" && $line =~ /;\s*$/) {
		$this->chopSemicolon;
		return 1;
	}

	return 0;
}

1;
