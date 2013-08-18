
=head1 NAME

Senora - Senoras main program

=head1 RESPONSIBILITIES

Init application. Host global variables

=head1 COLLABORATIONS

Instantiates global SessionMgr and PluginMgr (plus 
individual plugins). Instatitate MainLoop

=cut

# $Id: Senora.pm,v 1.3 2012/05/22 08:35:16 draumart Exp $

package Senora;

# extend inc so plugins are found
sub BEGIN {
	if ($Main::StandAlone) {return;}

	# duplicated code from Senora.pm
    	use Getopt::Long;
	Getopt::Long::Configure("no_ignore_case");
	Getopt::Long::Configure("bundling");
#	Getopt::Long::Configure("pass_through");

	$OPTS = {};
	GetOptions ($OPTS, ("s|silent", "o|oracleHome=s", "p|pathPrepend=s", "e|env=s", "h|help"));
	if ($OPTS->{h}) {
		print q(usage: perl Senora.pm [options] [connectString] [@script]);
		print qq(
		-s|--silent	 don't print welcome message
		-e|--env	 load environment file (".env") to set e.g. ORACLE_HOME
		-o|--oracleHome	
		-p|--pathPrepend prepend PATH by this directory (e.g. to use oracle instant client)
		-h|--help	 print this message
		);
		exit();
	}
	#print "@{[ %$OPTS ]}\n";	
	# --------------------------------------------------
	# extend path
	use File::Basename;
	use FindBin;

	my $pwd = dirname($FindBin::Bin);

	$pwd=~s,\\,/,g;
	if ($pwd=~/core/) {$pwd=dirname($pwd);}
	#print "pwd=$pwd\n";
	unshift (@INC, $pwd);
	unshift (@INC, "$pwd/core");
	unshift (@INC, "$pwd/plugins");
	unshift (@INC, "$pwd/local");
	#unshift (@INC, "$pwd/oci");

	# --------------------------------------------------
	# Load environment file
	if ($OPTS->{e}) {  
		$filename = "$pwd/environments/".$OPTS->{e}.".env";
		unless ( -r $filename) {
			print ("can't open '$filename'\n");
			exit(1);
		}				

		print "env=".$filename."\n"unless ($OPTS->{s});
		do "$filename";
	} else {
		$filename = "$pwd/environments/default.env";
		if( -r $filename) {
			print "env=".$filename."\n" unless ($OPTS->{s});
			do "$filename";
		}
	}
}

# This list included the stuff needed by some of the plugins
# Otherwise perl2exe is in trouble

use Carp;
use ConIo;
use Cwd;
use DBD::Oracle;
use DBI;
use DDL::Oracle;
use Exporter;
use Setting;
use Feedback;
use File::Basename;
use File::Spec;
use File::Spec::Win32;
use FileHandle;
use Getopt::Long;
use Misc;

use Plugin;
use PluginMgr;
use SqlFormatter;
use SqlSession;
use Stack;
use Statement;
use Text::ParseWords;
use Text::Wrap qw(wrap $columns $huge);
use Text::Wrap;
use strict;
use vars qw (
	     $OPTS
	     $PROMPT
	     $DEFAULT_PROMPT
	     $SESSION_MGR
	     $PLUGIN_MGR
	     $MAINLOOP
	     $HISTORY
	     @ISA
);


$DEFAULT_PROMPT = "no session> ";
$PROMPT=$DEFAULT_PROMPT;;




# ------------------------------------------------------------
sub welcome1 {
	# print initial (before any connections are made) message

	return if $OPTS->{s};
	print q(
SEN*Ora: Release 1.2.0.0.0 - Production from  22.05.2012 10:33
(c) Copyright 2008 Obstacle Concentration.  No rights reserved.
);
}


sub registerDefaultPlugins{
	# You can add plugins manually, but these are always there
	# some are stored in global variables

	# For full perl2exe version do not attempt to load *.pm files
	my $so = $Main::StandAlone;
	#print "Standalone = $so\n";

	$PLUGIN_MGR = PluginMgr->new;
	$MAINLOOP = $PLUGIN_MGR->registerPluginNamed("MainLoop", $so);

	$SESSION_MGR = $PLUGIN_MGR->registerPluginNamed("SessionMgr", $so);
	$PLUGIN_MGR->registerPluginNamed("Sqlplus", $so);
	$PLUGIN_MGR->registerPluginNamed("DataDictionary", $so);


	$HISTORY = $PLUGIN_MGR->registerPluginNamed("History", $so);
	$PLUGIN_MGR->registerPluginNamed("Bind", $so);
}

#------------------------------------------------------------
sub connectDb {
	# connect if connect string on cmdline

	if ($ARGV[0] && $ARGV[0] !~ "/nolog") {
		$SESSION_MGR->connect (\@ARGV);
		# skip connectString
		shift @ARGV;
	}
}

#------------------------------------------------------------
sub runCmdlineScript {
	# run script if found on cmdline xxx finish me

	my $cmdLine = "@ARGV";
	if ($cmdLine =~ s/^.*@//) {
		$PLUGIN_MGR->runCommand ('@ ' . $cmdLine);
	}
}

#------------------------------------------------------------
sub runLoginScript {
	# run login.sql if found

	my $loginFile = ($MAINLOOP->tryRunFile ("login.sra") || $MAINLOOP->tryRunFile ("login.sql"));

	print "Login file = $loginFile \n" if $loginFile;
}

#------------------------------------------------------------
sub breakHere {
	return 1;
}
#------------------------------------------------------------
sub main {
	# Create required objects and start MainLoop
	welcome1;
	registerDefaultPlugins;
	connectDb;

	runLoginScript();
	runCmdlineScript();


	while (1) {
		eval {
			breakHere();
			$MAINLOOP->onStdin();
		};
		print $@;
	}
}

$| = 1;
main();


