# ------------------------------------------------------
# NAME
#	SystemTools - non database utilities
# RESPONSIBILITIES
#
# COLLABORATIONS
#
# History
# ------------------------------------------------------
# $log$
# ------------------------------------------------------

package SystemTools;
use strict;
use Plugin;
use Misc;
use Cwd;
use File::Basename;
use File::Spec::Win32;
use File::Spec;
use File::Glob ':glob';

use vars qw 
(
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

	$this->{commands} = $COMMANDS;
	$this->{description} = qq(
Commands which do things outside the database.

);
	bless ($this, $class);

	return $this;
}



# ------------------------------------------------------------
$COMMANDS->{oracleHome} =
  {
   alias => "",
   description => "show or set ORACLE_HOME",
   synopsis => 
   [
    ["dir"]
   ],
   routine => \&oracleHome
  };

sub oracleHome {
	my ($this, $argv, $opts) = @_;
	my $home = $argv->[0];
	unless (-d $home) {
		print "No such directory $home.\n";
		return;
	}
	unless (-d "$home/bin") {
		print "No such directory $home/bin.\n";
		return;
	}
	# set environment
	$ENV{ORACLE_HOME} = $home;

	# prepend to path
	my $pathSep = ":";
	my $dirSep = "/";
	if (($^O eq 'MSWin32') || ($^O eq 'dos') || ($^O eq 'os2') ) {
		$pathSep = ";";
		$dirSep = "\\";

	}

	$ENV{PATH} = "$home".$dirSep."bin".$pathSep.$ENV{PATH};
}



# ------------------------------------------------------------
$COMMANDS->{setenv} =
  {
   alias => "",
   description => "show or set environment",
   synopsis => 
   [
    ["varname"],
    ["-s","value", "Set to value"]
   ],
   routine => \&setenv
  };

sub setenv {
	my ($this, $argv, $opts) = @_;
	my $variable = $argv->[0];
	print $variable . " = " . $ENV{$variable} . "\n";

	if (my $value = $opts->{"s"}) {
		$ENV{$variable} = $value;
		print $variable . " = " . $ENV{$variable} . "\n";
	}

}

# ------------------------------------------------------------
$COMMANDS->{which} =
  {
   alias => "",
   description => "find a file in path",
   synopsis => 
   [
    [""],
    ["-a",undef, "Print out all instances not just the first."],
    ["-p",undef, "Only print PATH"],
   ],
   man => "This program was originally written by Abigail 1999.",
   routine => \&which
  };

sub which {
	my ($this, $argv, $opts) = @_;


	my @PATH = ();
	my $PATHVAR = 'PATH';
	my $path_sep = ':';
	my $file_sep = '/';
	my @PATHEXT = ();

	my $Is_DOSish = ($^O eq 'MSWin32') ||
	  ($^O eq 'dos') ||
	    ($^O eq 'os2') ;

	if ($Is_DOSish) {
		$path_sep = ';';
	}
	if ($^O eq 'MacOS') {
		$path_sep = '\,';
		$PATHVAR = 'Commands';
		# since $ENV{Commands} contains a trailing ':'
		# we don't need it here:
		$file_sep = '';
	}

	# Split the path.
	if (defined($ENV{$PATHVAR})) {
		@PATH = split /$path_sep/ => $ENV{$PATHVAR};
	}
	# Add OS dependent elements.
	if ($^O eq 'VMS') {
		my $i = 0;
		my $path_element = undef;
		while (defined($path_element = $ENV{"DCL\$PATH;$i"})) {
			push(@PATH, $path_element);
			$i++;
		}
		# PATH may be a search list too
		$i = 0;
		$path_element = undef;
		while (defined($path_element = $ENV{"PATH;$i"})) {
			push(@PATH, $path_element);
			$i++;
    }
		# PATH and DCL$PATH are likely to use native dirspecs.
		$file_sep = '';
	}

	# trailing file types (NT/VMS)
	if (defined($ENV{PATHEXT})) {
		@PATHEXT = split /$path_sep/ => $ENV{PATHEXT};
	}
	if ($^O eq 'VMS') { @PATHEXT = qw(.exe .com); }

	if ($opts->{p}){
		foreach my $d (@PATH) {
			print "$d\n";
		}
		return;
	}

      COMMAND:
	foreach my $command (@$argv) {
		if ($^O eq 'VMS') {
			my $symbol = `SHOW SYMBOL $command`; # line feed returned
			if (!$?) {
				print "$symbol";
				next COMMAND unless $opts->{a};
			}
		}
		if ($^O eq 'MacOS') {
			my @aliases = split /$path_sep/ => $ENV{Aliases};
			foreach my $alias (@aliases) {
				if (lc($alias) eq lc($command)) {
					# MPW-Perl cannot resolve using `Alias $alias`
					print "Alias $alias\n";
					next COMMAND unless $opts->{a};
				}
			}
		}
		foreach my $dir (@PATH) {
			if ($^O eq 'MacOS') {
				if (-e "$dir$file_sep$command") {
					print "$dir$file_sep$command\n";
					next COMMAND unless $opts->{a};
				}
			}
			else {
				if (-x "$dir$file_sep$command") {
					print File::Spec->canonpath("$dir/$command\n");
					next COMMAND unless $opts->{a};
				}
			}
			if (@PATHEXT) {
				foreach my $ext (@PATHEXT) {
					if (-x "$dir$file_sep$command$ext") {
						print File::Spec->canonpath("$dir/$command$ext\n");
						next COMMAND unless $opts->{a};
					}
				}
			}
		}
	}

}

# ----------------------------------------
$COMMANDS->{"host"} = 
  {
   alias => "",
   description => "execute os cmd",
   synopsis => [
		["cmd"],
		],
   routine => \&host,
   };

sub host {
	my ($this, $argv, $opts) = @_;
	my $args = "@$argv";

	return unless $args;

	system($args);
}

# ----------------------------------------
$COMMANDS->{"cd"} = 
  {
   alias => "",
   description => "change directory",
   synopsis => [
		["cmd"],
		],
   man => q(
	The trouble, when 'you' were in one directory, and your
	scripts were in another directory (millions of slashes away),
	has finally come to an end.
   ),
   routine => \&cd,
   };

sub cd {
	my ($this, $argv, $opts) = @_;
	my $args = "@$argv";

	return unless $args;

	chdir ($args) or print "cannot cd to $args.\n";
}

# ----------------------------------------
$COMMANDS->{"pwd"} = 
  {
   alias => "cwd|dirs",
   description => "show current directory",
   synopsis => [
		[],
		],
   man => q(
	Pwd prints the current working directory. The 'dirs' alias is
	especially useful, when you run senora from withing emacs (for
	'shell-resync-dirs')
   ),

   routine => \&s_pwd,
   };

sub s_pwd {
	my ($this, $argv, $opts) = @_;
	my $args = "@$argv";

	print cwd."\n";
}

# ----------------------------------------
$COMMANDS->{"dir"} = 
  {
   alias => "",
   description => "list directory",
   synopsis => [
		["pattern"],
		],
   man => q(
	This stupid little command runs 'dir' on DOS-like systems and
	'ls' on all others. Options are passed literally so 'dir -l'
	will run an 'ls -l'.

	Too bad the name 'ls' was already in use for DataDictionary
	listing. Well ... you can always run 'host ls -l'.
   ),
   routine => \&dir,
   };

sub dir {
	my ($this, $argv, $opts) = @_;
	my $pattern = "@$argv";

	if ($^O =~ /win/i) {
		system ("dir $pattern");
	} else {
		system ("ls $pattern");
	}

}

# ----------------------------------------
$COMMANDS->{"perl"} = 
  {
   alias => "",
   description => "run a perl command or script",
   synopsis => [
		["some Strings"],
		["-f", undef, "treat some Strings as filename and arguments"],
		["-v",undef, "be vebose"],
		],
   man => qq(
	Execute a perl command or run a perl script with
	arguments. Note that the -v and -h options will not be passed
	to the script because they are already eaten away by Senora.

	This is primarily useful for users of Senora for
	Windows (compiled version).
   ),
   routine => \&perl
   };

sub perl {
	my ($this, $argv, $opts) = @_;
	local @ARGV;
	if ($opts->{f}) {
		print "running script @$argv \n" if $opts->{v};
		my $filename = $argv->[0];
		unless (-r $filename) {
			print ("can't open '$filename'\n");
			return;
		}
		@ARGV=@$argv;
		do "$filename";
	} else {
		my $command = "@$argv;";
		print "running command '$command' \n" if $opts->{v};
		eval "$command";
	}
	print "$@\n";


}

# ----------------------------------------
sub getOracleHome {
	my $home;
	if ($home = $ENV{ORACLE_HOME}) {
		return $home;
	}

	my @dirs = split/;/,$ENV{PATH};
	foreach my $d(@dirs) {
		if (-X "$d/oracle.exe") {
			my $OracleHome = dirname($d);
			return ($OracleHome);
		}
	}

	print "Can't figure out ORACLE_HOME\n";
	return undef;
}

sub getOracleHomeForwardSlashed {
	my $home = getOracleHome;
	$home =~ s/\\/\//g;
	#print "$home\n";
	return $home;
}


# ----------------------------------------
$COMMANDS->{"tnsnames"} = 
  {
   alias => "tns",
   description => "list tnsnames entire",
   synopsis => [
		["pattern"],
		["-l",undef, "long format"],
		["-H",undef, "print HOST"],
		["-e",undef, "edit TNSNAMES"],
		],
   routine => \&tnsnames
   };

sub tnsnames {
	my ($this, $argv, $opts) = @_;
	my $oracle_home;
	my $pattern = $argv->[0];
	$oracle_home=getOracleHomeForwardSlashed();
	unless ($oracle_home) {
		print "ORACLE_HOME not set\n";
		return;
	}
	foreach my $d ( $ENV{TNS_ADMIN},
	  ".",							# current directory
	  "$oracle_home/network/admin",	# OCI 7 and 8.1
	  "$oracle_home/net80/admin",	# OCI 8.0
	  "/var/opt/oracle"
	) {
	    next unless $d && open(FH, "<$d/tnsnames.ora");
	    print "# $d/tnsnames.ora \n";
	    my $foundIt;
	    if ($opts->{e}) {
		    Sqlplus::callEditor("$d/tnsnames.ora");
	    } else {
		    while (<FH>) {
			    m/^\s*([-\w\.]+)\s*=/;
			    my $name = $1;
			    if ($name) {
				    $foundIt = $name =~ /$pattern/i;
			    }
			    print "# *** $name ***\n" if $name and $foundIt;
			    if ($foundIt and $opts->{l}) {
				    print "$_";
			    }
			    
			    if ($foundIt and $opts->{H}) {
				    if (m/.*(HOST = [0-9\w\.]*).*/i) {
					    print "# $1\n" if $1;
				    }
			    }
		    }

	    }
	    close FH;
	    last;
	}
    }

# ----------------------------------------
$COMMANDS->{"genDrop"} = 
  {
   alias => undef,
   description => "generate drop statements from create files",
   synopsis => [
		["file"],
		["-f", "outputFile", "save to file"],
		["-P", undef, "generate pl/sql avoiding drop errors"],
		],
   routine => \&genDrop
   };

sub genDrop {
	my ($this, $argv, $opts) = @_;
	my $file = $argv->[0];
	my @drops;
	my $types='CLUSTER|CONSUMER GROUP|CONTEXT|DATABASE LINK|DIMENSION|DIRECTORY|EVALUATION CONTEXT|FUNCTION|INDEX|INDEX PARTITION|INDEXTYPE|JAVA CLASS|JAVA DATA|JAVA RESOURCE|JAVA SOURCE|LIBRARY|LOB|MATERIALIZED VIEW|OPERATOR|PACKAGE|PACKAGE BODY|PROCEDURE|QUEUE|RESOURCE PLAN|RULE SET|SEQUENCE|SYNONYM|TABLE|TABLE PARTITION|TRIGGER|TYPE|TYPE BODY|VIEW|XML SCHEMA';
	open IFD, "$file" or die "cannot open $file";
	while (<IFD>) {
		chomp;
		if ($_ =~ /^\s*(create\b)/i) {
			if ($_ =~ /^\s*(create)\s*(or\sreplace|unique)*\s($types)\s*(\w+).*/i) {
				my $type = $4 ? $3 : $2;
				my $name = $4 ? $4 : $3;
				push (@drops, [$type,$name]);
			} else {
				# a CREATE we cannot handle.
				push (@drops, ["-- cannot handle:", $_ ]);
			}
		}
	}

	my $ofile = $opts->{f};
	if ($ofile) {
		open OFD, ">$ofile" or die "cannot open $ofile";
	}

	if ($opts->{P}) {
		my $txt = qq<
begin
	for o in (
		select object_type, object_name
		from user_objects
		where object_name in (
>;
		print $txt;
		print OFD $txt if ($ofile);
		my $sep;
		foreach my $i (reverse (@drops)) {
			$txt = "$sep\t\t\t'". uc($i->[1])."'";
			print $txt;
			print OFD $txt if ($ofile);
			$sep=",\n";
		}
		$txt = qq<
		)
		and object_type != 'PACKAGE BODY'
		and object_type != 'TABLE PARTITION'
		and object_type != 'INDEX PARTITION'
	) loop
		if (o.object_type = 'TABLE') then
			execute immediate ('drop '||o.object_type||' '||o.object_name|| ' cascade constraints');
		else
			execute immediate ('drop '||o.object_type||' '||o.object_name);
		end if;
	end loop;
end;
/
>;
		print $txt;
		print OFD $txt if ($ofile);
	} else {
		foreach my $i (reverse (@drops)) {
			my $stmt = "drop $i->[0] $i->[1];\n";
			if ($ofile) {
				print OFD $stmt;
			}
			print $stmt;
		}
	}

	close IFD;
	close OFD if ($ofile);
}



1;
