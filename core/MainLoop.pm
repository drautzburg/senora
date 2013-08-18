=head1 NAME

Input - Input main loop

=head1 RESPONSIBILITIES
Find and ppen files (incl stdin).

Build statements from lines and excute them. Take care of
prompting. Strip comments. Do ampersand replacing. Provide commands to
run scrips.

=head1 COLLABORATIONS
Writes line to Statement

=cut

# $Id: MainLoop.pm,v 1.1 2011/03/25 14:36:23 ekicmust Exp $

package MainLoop;
use Settings;
use Misc;
use ConIo;
use Statement;
use strict;
use FileHandle;
use File::Basename;
use Feedback;
use vars qw 
(
 @ISA
 $COMMANDS
@MainLoops
);

@ISA = qw (
	   Plugin
);


# ------------------------------------------------------------
sub new {
	my ($Input) = @_;
	my $class = ref($Input) || $Input;
	my $this={
		  commands => $COMMANDS,
		  statement => Statement->new(),
		  sqlMode => undef,
		  inComment => 0,
		  filename => undef,
		  isStdin => 0,
		  fd => undef
		 };
	bless ($this, $class);
	return $this;
}

# ----------------------------------------
sub activeMainLoop {
	my ($this, $newLoop) = @_;
	if ($newLoop) {
		push (@MainLoops, $newLoop)
	}
	return $MainLoops[-1];
}

# ----------------------------------------
sub onFile {
	my ($this, $filename) = @_;
	$this = $this->new();
	my $fd = $this->{fd} = FileHandle->new($filename);
	$this->{filename} = $filename;
	unless ($fd) {
		print ("Cannot open \'$filename\' for reading.\n");
		return undef;
	}
	$this->mainloop();
}

# ----------------------------------------
sub onStdin {
	my ($this) = @_;
	$this = $this->new();
	my $fd;
	unless ($fd = $this->{fd} = $this->getReadlineSupport()) {
		$fd = $this->{fd} = FileHandle->new("-");
	} 
	$this->{filename} = "-";
	unless ($fd) {
		print ("Cannot open stdin for reading.\n");
		return undef;
	}
	$this->{isStdin} = 1;
	$this->mainloop();
}

# ----------------------------------------
sub scriptCwd {
	# return the current working directory of running script
	my ($this) = @_;
	return "." if ($this->{isStdin});

	return dirname($this->{filename});
}
# ----------------------------------------
$COMMANDS->{start} =
  {
   alias => '@',
   description => "run an sql script",
   synopsis => [
		["args"],
		],
   routine => \&runScriptAbs,
   };

$COMMANDS->{startRel} =
  {
   alias => '@@',
   description => "run an sql script relative to a running script",
   synopsis => [
		["args"],
		],
   routine => \&runScriptRel,
   };

sub runScriptAbs {
	runScript (@_);
}

sub runScriptRel {
	runScript (@_, {r => 1});
}

sub runScript {
	my ($this, $argv, $opts, $opts) = @_;

	if (my $fqName = $this->findFile ($argv->[0], $opts)) {
		shift @$argv; 
		$this->setArgv ($argv);
		$this->onFile($fqName);
	} else {
		print "cannot open $argv->[0]\n";
		Feedback::handleWheneverSqlerror();
	}
}

#----------------------------------------
sub findFile {
	# open file for readig and run it. Return
	# fd. With -r set, look in scriptCwd.

	my ($this, $filename, $opts) = @_;

	# add .sql to unqualified filenenames except "-"
	$filename .= ".sql" unless (basename($filename) =~ /\./i);

	# figure out where to look for the file
	if ($filename =~ /^\//) {
		# take filename as is
	} elsif ($opts->{r} && (my $dir = $this->scriptCwd)) {
		$filename = "$dir/$filename";
	} else {
		# unqualified pathname: try SQLPATH
		my $path = ".:".$ENV{SQLPATH};

		# windows
		$path =~ s/;/:/; 

		my $drive;
		foreach my $dir (split (":",$path)) {
			# a drive letter looks like a dir to me
			if ($dir=~/^[A-Za-z]$/) {
				$drive="$dir:";
				next;
			}

			if (-r "$drive$dir/$filename") {
				$filename = "$drive$dir/$filename";
				last;
			}
			$drive=undef;
		}
	}
	if ($opts->{v}) {
		print "$filename\n";
	}
	if (-r $filename) {
		return $filename;
	} 
	return undef;
}

#----------------------------------------
sub tryRunFile {
	# find file and run it if found
	my ($this, $filename) = @_;
	if (my $fqName = $this->findFile($filename)) {
		$this->onFile($fqName);
		return $fqName;
	}
	return 0;
}
#----------------------------------------
sub isaTty {
	# return true if current Fd is a tty;
	my ($this) = @_;

	return 0 unless $this->{isStdin};

	# emacs shell mode will not be considered a tty under windows
	# we know better. This may lead to unwanted line numbers when
	# input is piped into senora, but we get wanted line number
	# when running interactively 

	if ($ENV{TERM} eq "emacs" && $^O =~ /win/i) {
		return 1;
	}
	my $fd = $this->{fd};

	# same with readline: -t says: "not a tty", but it is

	if (ref($fd) =~ /Term::ReadLine/) {
		return 1 
	}
	return ( -t $fd);
}

#----------------------------------------
sub getReadlineSupport {
#----------------------------------------
	my ($this) = @_;


	my $term;
	my $history_file= $ENV{HOME}."/.senora_history";
	if ($ENV{TERM} ne "emacs" && $^O !~ /win/i) {
		eval (q(
		       use Term::ReadLine;

		       $term = new Term::ReadLine 'SEN';
		       my %attribs;
		       my $attribs = $term->Attribs;
		       $term->MinLine(1);
			   if($] >=  5.006) {
		       $term->stifle_history(80);
			   }
		       $attribs{do_expand}= 1;
			   if($] >=  5.006) {
		       $term->ReadHistory($history_file);
			   }
		      ));
	}

	if ($term) {
		print "Readline support enabled.\n" unless ($Senora::OPTS->{"s"});
		return $term;
	} else {
		print "No readline support.\n" unless ($Senora::OPTS->{"s"});
		return undef;
	}
}

#----------------------------------------
sub haveReadline {
#----------------------------------------
	my ($this) = @_;
	return undef unless $this->{fd};
	return (ref($this->{fd}) =~ /Term::ReadLine/) ;
}

#----------------------------------------
sub getline {
	# read the next line from my fd
	
	my ($this, $prompt)= @_;
	my $line;

	my $fd = $this->{fd};

	# fixed by chipach
	if ($this->isaTty) {
		do {
			if ($this->haveReadline()) {
				$line = $fd->readline($prompt);
			}
			else {
				print ($prompt);
				$line = $fd->getline;
				chomp($line);
			}
		} until $line;
	} else {
		$line = $fd->getline;
	}

	if ($line) {
		# make sure we have a newline even at eof
		chomp($line);
		return "$line\n"; 
	}

	return (undef);
}


#----------------------------------------
sub setArgv {
	# set the &1..&n defines when running a script with params
	my ($this, $args) = @_;

	my $n = 1;
	foreach my $arg (@$args) {
		define->set($n, $arg);
		$n++;
	}
}


#----------------------------------------
sub handleComments {
	# strip comments. Return 0 is nothing is left 1 otherwise
	my ($this, $line) = @_;

	# skip "rem" comments
	if ($line =~ /^\s*rem/i){
		return "";
	}
	# skip "--" comments
	if ($line =~ /^\s*--/i){
		return "";
	}

	# a comment must start at the beginning of the line or be
	# preceded by whitespace. Otherweise there would be a problem
	# with commands like "ls table/*".

	my $commentStart=q((^|\s+)\/\*);
	my $commentEnd=q(\*\/);
	my $comment;
	my $remainder;
	if ($this->{inComment}) {
		$remainder = undef;
		$comment = $line;

		if ($line =~ /(.*)$commentEnd(.*)/) {
			$remainder=$2;
			$comment = $1;
			$this->{inComment}--;
		}
		else {
		}
	}
	else {
		$remainder = $line;
		$comment = undef;

		if ($line =~ /^(.*)$commentStart(.*)$commentEnd(.*)/) {
			$remainder=$1.$3;
			$comment = $2;
		} else {
			if ($line =~ /^(.*)$commentStart(.*)/) {
				$remainder=$1;
				$comment = $2;
				$this->{inComment}++;
				}
		}
	}

	if ($comment) {
		print "doc> $comment ";
	}
	if ($remainder) {
		return "$remainder";
	}
	return "";

}

#----------------------------------------
sub nextStatement {
	# return the next complete statement (with sqlMode set)

	my ($this) = @_;
	$| = 1;
	my $lineNo = 1;
	my $stmt = Statement->new;
	my $line;

	Sqlplus::getServeroutput();

	my $prompt=$Senora::PROMPT;
	while ($line=$this->getline($prompt)) {
		if ($lineNo == 1) {
			$line = $this->handleComments($line);
			# skip blank lines
			if ($line =~ "^[ 	]*\$") {
				next;
			}

			# add a space after leading "@" of "@@" to
			# make it a real command

			$line =~ s/^(\s*(\@+))/$1 /;
			$line =~ s/^(\s*(\!+))/$1 /;

			$stmt->setMode($line) or next;
		}

		# let Statement accumulate the lines
		#print "got: $line";
		$stmt->addline($line);
		if ($stmt->endOfStatement) {
			return $stmt;
		} else {
			
			# for multiline statements print linenumbers
			$prompt=$lineNo++." ";
		}
	}
}

#----------------------------------------
#use sigtrap qw(handler controlC normal-signals handler controlC BREAK);

#sub controlC {
#	print "caught\n";
#	exit;
#}

#----------------------------------------
sub tryMyCommandsFirst {
	# run my commands immediately (credits to Tom Korinth for this idea)
	# This saves us a lot of grief, maintaining a file descriptor stack
	my ($this, $statement) = @_;

	my $cmdline = $statement->get();
	my $cmd = $this->canRunCommand($cmdline);
	$cmd = $this->canRunAlias($cmdline) unless ($cmd); 

	if ($cmd){
		$this->runCmdline($cmd, $cmdline);
		return 1;
	}
	return 0;
}
#----------------------------------------
sub mainloop {

	my ($this) = @_;
	     
	while (my $s = $this->nextStatement()) {
		$Senora::HISTORY->add($s);
		unless ($this->tryMyCommandsFirst($s)) {
			# no this one wan't for me. 
			$s->run;
		}
	}
}

1;
