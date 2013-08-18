
=head1 NAME

SenoraForWindows - Wrapper for perl2exe

=head1 RESPONSIBILITIES

Load essential Plugins statically. This will let perl2exe create one
executable with the essential plugins already loaded. However you
cannot change these anymore.

This is useful if you want to use Senora as a scripting engine to
execute just one plugin. Oracle support uses Visual Basic in
situations like these.

=head1 COLLABORATIONS

Senora

=cut


BEGIN {
	$Main::StandAlone=1;

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
	my $pwd = dirname($0);
	$pwd=~s,\\,/,g;
	if ($pwd=~/core/) {$pwd=dirname($pwd);}
	#print "pwd=$pwd\n";
	unshift (@INC, $pwd);
	unshift (@INC, "$pwd/core");
	unshift (@INC, "$pwd/plugins");
	unshift (@INC, "$pwd/local");
	unshift (@INC, "$pwd/oci");

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

		unless ($OPTS->{s}) {print qq(
This version was compiled using perl2exe courtesy of indigostar.
See http://www.indigostar.com/perl2exe.htm
);}

}


use DBI;
use DBD::Oracle;

use PluginMgr;
use MainLoop;
use SessionMgr;
use SqlFormatter;
use PluginMgr;

use Setting;
use Settings;
use MainLoop;
use SessionMgr;
use Sqlplus;
use DataDictionary;
use History;
use Bind;
use Senora;
use SQL::Beautify

use Win32API::Registry 0.12 qw( :KEY_ :HKEY_ :REG_ );
#use Tie::Registry;
use File::Basename;
use File::Spec::Win32;
use File::Spec;
use File::Glob ':glob';
use Win32::TieRegistry;
use Posix;
use DirHandle;


