# ------------------------------------------------------
# NAME
#	Load legacy SQL scripts as plugings
# RESPONSIBILITIES
#
# COLLABORATIONS
#
# History
# ------------------------------------------------------
# $log$
# ------------------------------------------------------

=head1 NAME

Load legacy SQL scripts as plugings

=head1 RESPONSIBILITIES

=head1 COLLABORATIONS

=cut

package LegacySql;
use Misc;
use strict;
use Plugin;
use Settings;
use File::Basename;
use SessionMgr qw(sessionNr sessionMain sql);
use FileHandle;
use DirHandle;
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

	$this->{description} = qq(

Commands which allow turning SQL-code into Senora commands. If you
have a collection of useful SQL scrips, then this plugin lets you
organize them. );

	bless ($this, $class);
	return $this;
}


# ------------------------------------------------------------
$COMMANDS->{lregister} =
  {
   alias => "",
   description => "register legacy SQL scripts as plugins",
   synopsis => [
		["fileOrDir"],
		],
   man => qq(Loads a file or all sql amd sra file in a directory and makes them 
	available as new commands. The command name will be the name of the
	file unless the file contains a senora header.

	This means that you can
	- search for the mew command with "help"
	- you can print documentation with <command> -h or
	  SenoraDocHtml
	- you can pass options to the script

	  A senora header consists of the following keywords:
	-- SenonraCommand: 
	-- Description:
	-- Alias: 
	--| (followed by a line of manpage)
	-- args: &define 
	-- -x &define comment (where -x stands for a single char option)

	Before actually running the script Senora will push all
	settings and restore them later. This means that things like
	column formats and &defines will have no effect on subsequent
	commands. They will however remain active for scripts called
	from the script.


),
   routine => \&lregister,
   };

sub lregister {
	my ($this, $argv, $opts) = @_;
	my $file=$argv->[0];

	if (-d $file) {
		$this->addDirectory($file);
	} else {
		$this->addFile($file);			
	}

}

# ------------------------------------------------------------
sub turnOptionsIntoDefines {
	my ($this, $argv, $opts, $synopsis) = @_;
	if ($argv) {
		my $define = $synopsis->[0]->[0];
		# strip the ampersand
		$define =~ s/^\&//;
		define->set($define, $argv->[0]);
	}

	my @synopsis = @$synopsis;
	# strip off the argv
	shift (@synopsis);

	foreach my $so (@synopsis) {
		# get the flag ("-x") and the &defineName
		my $flag = $so->[0];
		$flag =~ s/-//;
		my $defineName = $so->[1];
		# strip the ampersand
		$defineName =~ s/^\&//;
		# print "$flag. $defineName \n";
		# if the option was entered, place a &define
		if ($opts->{$flag}) {
			define->set($defineName, $opts->{$flag});
		}
	}
	
}

# ------------------------------------------------------------
sub keyword {
	# parse a line in the style: "  -- key theValue of theKey"
	my ($line, $key) = @_;
	if ($line =~ /^\s*\-\-\s*$key\s*(.*)/) {
		return $1;
	}

}

# ------------------------------------------------------------
sub readOracleDescription {
	# this works for most Oracle supplied scripts
	my ($this, $fd) = @_;
	# read text after description
	my $man;
	my $line++;
	while (<$fd>) {
		$line++;
		# Stop at an all uppercase line
		if ($_ =~ /Rem\s*[A-Z]{4,10}\s$/) {
			return $man;
		}
		if ($_ =~ /^Rem\s*(.*)/) {
			$man .= "$1\n\t";
		}

		if ($line > 100) {
			return $man
		}
	}
}
# ------------------------------------------------------------
sub parseLegacySqlDefault{
	# construct a COMMAND hashref into $s
	# return command if specified
	# this routine encapsulates all file-header conventions

	my ($this, $s, $fd) = @_;
	my $command;
	my $line = 0;
	while (<$fd>) {
		$line++;
		my $keyValue;
		if ($keyValue = keyword($_, "SenoraCommand:")) {
			$command = $keyValue;
		}
		if ($keyValue = keyword($_, "Description:")){
			$s->{description} = $keyValue;
		}
		if ($keyValue = keyword($_, "\\|")){
			$s->{man} .= $keyValue."\n\t";
		}
		if ($keyValue = keyword($_, "Alias:")){
			$s->{alias} .= $keyValue;
		}
		if ($keyValue = keyword($_, "args:")){
			$s->{synopsis}->[0] = [$keyValue];
		}
		# -x style options. Example:
		#-- -c: &column the column to select from
		if ($_ =~ /^\s*\-\-\s*(\-.):\s*(\&\w+)\s*(.*)/){
			# (sorry for that stuff above)
			#print qq(kv="$1" "$2" "$3"\n);
			push (@{$s->{synopsis}}, [$1,$2,$3]);
		}
		# try Oracle style description
		if ($_ =~ /Rem\s*DESCRIPTION/) {
			$s->{man} .= $this->readOracleDescription($fd);
		}
		last if ($line > 100)
	}

	return $command;
}

# ------------------------------------------------------------
sub initSynopsis {
	my ($this, $fileName) = @_;

	# initialize the hashref with some defaults
	my $s = {};
	$s->{alias} = "";
	$s->{description} = "runs " . basename($fileName);
	$s->{synopsis} = [[]];

	$s->{routine} = sub {
		my ($this, $argv, $opts) = @_;
		$this->runFile($fileName, $argv, $opts, $s->{synopsis});
	};
	return $s;
}

# ------------------------------------------------------------
sub postprocessSynopsis {
	my ($this, $s, $fileName) = @_;
	# add some more default stuff
	$s->{description} = "runs $fileName" unless ($s->{description});
	$s->{man} .= "($fileName)";
	push (@{$s->{synopsis}}, ["-P", undef, "print " . basename($fileName)]);
	push (@{$s->{synopsis}}, ["-E", undef, "edit " . basename ($fileName)]);

}

# ------------------------------------------------------------
sub printFile {
	my ($this, $fileName) = @_;
	my $fd = FileHandle->new($fileName);
	unless ($fd) {
		print "Cannot open $fileName.\n";
		return;
	}
	while (<$fd>) {
		print;
	}
	print "\n";
	$fd->close();
}

# ------------------------------------------------------------
sub editFile {
	my ($this, $fileName) = @_;
	Sqlplus::callEditor($fileName);
}

# ------------------------------------------------------------
sub addFile {
	my ($this, $fileName) = @_;

	my $fd = FileHandle->new($fileName);
	unless ($fd) {
		print "Cannot open $fileName\n";
		return;
	}

	# create a synopsis data structure
	my $s = $this->initSynopsis($fileName);
	my $commandFromFile = $this->parseLegacySqlDefault ($s, $fd);
	$this->postprocessSynopsis ($s, $fileName);
	$fd->close();

	# get the command name right
	my $commandName = $commandFromFile ? $commandFromFile : basename($fileName,(".sql", ".sra"));

	$this->{commands}->{$commandName} = $s;
	my $alias = $s->{alias};
	$alias = " (" . $alias . ")" if ($alias);
	print basename ($fileName) . " registered as $commandName$alias.\n";
}

# ------------------------------------------------------------
sub addDirectory {
	my ($this, $dirName) = @_;

	my $fd = DirHandle->new($dirName);
	unless ($fd){
		print "Cannot read $dirName.\n" unless ($fd);
		return;
	}
	while (my $file = $fd->read()) {
		if ($file =~/.*\.(sql|sra)$/) {
			$this->addFile("$dirName/$file");
		}
	}
}

# ------------------------------------------------------------
sub runFile {
	my ($this, $fileName, $argv, $opts, $synopsis) =  @_;
	if ($opts->{"P"}) {
		$this->printFile($fileName);
		return;
	}
	if ($opts->{"E"}) {
		$this->editFile($fileName);
		return;
	}

	settings->push();
	$this->turnOptionsIntoDefines ($argv, $opts, $synopsis);
	$Senora::MAINLOOP->runScriptAbs([$fileName]);
	settings->pop();
}


1;
