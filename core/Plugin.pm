
=head1 NAME

Plugin - Base class for Plugins

=head1 RESPONSIBILITIES

Provide methods common to all Plugins.

=head1 COLLABORATIONS

Used by all Plugins, i.e. things that are available as commands.

=cut

# $Id: Plugin.pm,v 1.2 2012/03/16 17:47:46 draumart Exp $

package Plugin;
use strict;
use Getopt::Long;
use Text::ParseWords;
Getopt::Long::Configure("no_ignore_case");
Getopt::Long::Configure("bundling");
Getopt::Long::Configure("pass_through");


use vars qw (
	@ISA
);

# ------------------------------------------------------------
sub new {
	my ($That) = @_;
	my $class = ref($That) || $That;
	my $this = {};
	bless ($this, $class);
	return $this;
}


# ------------------------------------------------------------
sub allCommands {
	# return an array of all commands

	my ($this) = @_;
	my $commands = $this->{commands};

	return keys (%$commands);
}

# ------------------------------------------------------------
sub noRegexp {
	#strip spaces and stars from cmd names
	my ($cmd) = @_;
	$cmd =~ s/[\* 	]+/ /g;
	return $cmd;
}

# ------------------------------------------------------------
sub mkPattern {
	# buld a regexp that allows leading whitespace and a semicolon
	my ($this, $pattern) = @_;
	$pattern = "^\\s*($pattern)[\\s;]+\|^\\s*($pattern)\\s*\$";
	return $pattern;
}
# ------------------------------------------------------------
sub canRunGeneric {
	# Answer if any of my commands can run cmdline. Return command
	# (not alias) if thats the case

	# Watchout: PluginMgr is a plugin itself.

	my ($this, $cmdline, $asAlias) = @_;
	my $commands = $this->{commands};
	my $pattern;

	foreach my $command ($this->allCommands) {
		if ($asAlias) {
			$pattern = $commands->{$command}->{alias};
		} else {
			$pattern = $command
		}

		next unless $pattern;
		# buld a regexp that allows leading whitespace
		$pattern = $this->mkPattern($pattern);
#print "'$cmdline' $pattern \n";
		if ($cmdline =~ /$pattern/i) {
			return $command;
		}
	} 
	return 0;
}

# ------------------------------------------------------------
sub canRunCommand {
	my ($this, $cmdline) = @_;
	return $this->canRunGeneric($cmdline, 0);
}
# ------------------------------------------------------------
sub canRunAlias {
	my ($this, $cmdline) = @_;
	return $this->canRunGeneric($cmdline, 1);
}

# ------------------------------------------------------------
sub runCmdline {
	my ($this, $cmd, $cmdline) = @_;

	$cmdline = $this->stripCommand($cmd, $cmdline);
	my @argv = $this->createArgv($cmdline);
	my @optspec = $this->createOptionSpec($cmd);
	my ($argv, $opts) = $this->getOpt(\@argv, \@optspec);
	my $routine = $this->{commands}->{$cmd}->{routine};

	if ($opts->{h}) {
		$this->printSynopsis($cmd);
	} else {
		# run the command
		&$routine($this, $argv, $opts);
		
	}

}

# ------------------------------------------------------------
sub stripCommand {
	# return cmdline with command itself removed
	my ($this, $cmd, $cmdline) = @_;

	my $cmdPattern = $this->mkPattern($cmd);
	my $aliasPattern = $this->mkPattern
	  (
	   $this->{commands}->{$cmd}->{alias}
	  );

	($cmdline =~ s/$cmdPattern//i) || ($cmdline =~ s/$aliasPattern//i);
	return $cmdline;
}
# ------------------------------------------------------------
sub createArgv {
	# create ARGV with each word as one element
	# leave quoted strings intact
	my ($this, $cmdline) = @_;

	chomp($cmdline);

	# split at whitespace, goup double quoted strings to singe arg
	# escape single quotes so they remain

	#$cmdline =~ s/\'/\\'/g; #' (emacs font-lock)
	my @argv = quotewords(" +", 1, $cmdline);
	#print "argv = @argv\n";
	return @argv;
}

# ------------------------------------------------------------
sub createOptionSpec{
	# create option spec for GetOptions() from synopsis

	my ($this, $command) = @_;
	my $thisCommand = $this->{commands}->{$command} or die;

	my $synopsis = $thisCommand->{synopsis};

	my @option_spec = ("-h"); # -h is always possible;
	my $os;
	for (my $i=1; $synopsis->[$i]; $i++) {
		$os = $synopsis->[$i]->[0];
		$os .= "=s" if $synopsis->[$i]->[1];
		push (@option_spec, $os);
	}
	return @option_spec;
}

# ------------------------------------------------------------
sub getOpt {
	# run GetOptions, return (\@argv, $opts) with remaining args in argv

	my ($this, $argv, $optionSpec) = @_;

	my @argv = @$argv;

	local @ARGV;
	@ARGV = @argv;
	my $opts = {};
	GetOptions ($opts, @$optionSpec);

	# remaining args go into argv
	@argv = @ARGV;

	return (\@argv, $opts);
}

# ------------------------------------------------------------
sub printSynopsis {
	my ($this, $command) = @_;
	my $thisCommand = $this->{commands}->{$command};
	my @synopsis = @{$thisCommand->{synopsis}};

	print "NAME\n\t".noRegexp($command)." - $thisCommand->{description}\n";
	print "\nSYNOPSIS\n\t".noRegexp($command)." <$synopsis[0][0]>\n";
	print "\talias:\t$thisCommand->{alias}\n" if $thisCommand->{alias};

	# now print the option specs
	shift(@synopsis);
	foreach my $s (@synopsis) {
		printf ("\t%-3s%-12s%s\n", $s->[0], $s->[1], $s->[2]);
	}
	# now print the man if any
	if (my $man = $thisCommand->{man}) {
		$man =~ s/^\s*/\t/m;
		$man =~ s/^\s$//m;
		print "\nDESCRIPTION\n$man" 
	}
	my $file = $this->{FILE} ? $this->{FILE} : ref($this).".pm";
	print "\nFILES\n\t$file\n"
}


# ------------------------------------------------------------
sub printAllSynopsis {
	my ($this) = @_;
	foreach my $command ($this->allCommands) {
		$this->printSynopsis($command);
		print "\n";
	}
}
# ------------------------------------------------------------
sub helpCommand {
	# provide one-line help
	my ($this, $argv, $opts) = @_;

	my $pattern = $argv->[0] ? $argv->[0] : ".";

	my $plugin = ref($this);
	my $printedHeader = 0;

	my @allCommands = sort($this->allCommands);
	
	foreach my $cname (@allCommands) {
		my $cmd = $this->{commands}->{$cname};
		my $alias = $cmd->{alias};
		my $descr = $cmd->{description};

		if ("$cname.$alias.$descr.$plugin" =~ /$pattern/i) {
			my $l = "------------------------------------------";
			print "\n$l\n-- $plugin:\n" unless $printedHeader;
			$printedHeader = 1;
			if ($opts->{v}) {
				print "- - - - - - -\n";
				$this->printSynopsis($cname);
				print "\n";
			} else {
				printf ("%-20s %-12s - %s\n", 
					noRegexp($cname), 
					$alias ? "(".$alias.")" : "",
					$descr);
			}
		}
	}
}


# ------------------------------------------------------------
sub noArgs {
	# answer if function was called with no arguments or options
	# call as: this->noArgs(@_);

	my ($this, $this, $argv, $opts) = @_;
	my @keys = keys(%$opts);
	if ( ($#$argv == -1) and ($#keys == -1)) {
		#print "noargs\n";
		return 1;
	}
	return 0;
}

1;
