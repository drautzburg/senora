
=head1 NAME

PluginMgr - manage plugins

=head1  RESPONSIBILITIES

Provide methods to register and run Plugins.  Provide help. Find the
plugin that can run a given command.

=head1  COLLABORATIONS

PluginMgr has its own builtin commands and is thus a plugin
itself.

=cut

# $Id: PluginMgr.pm,v 1.2 2012/03/16 17:47:46 draumart Exp $

package PluginMgr;
use FileHandle;
use Settings;
use Misc;
use strict;
use vars qw (
	     @ISA
	     $COMMANDS
);

@ISA = qw (
	   Plugin
);

# ------------------------------------------------------------
sub new {
	my ($PluginMgr) = @_;
	my $class = ref($PluginMgr) || $PluginMgr;
	my $this={};

	# commands of PluginMgr itself;
	$this->{commands} = $COMMANDS;

	bless ($this, $class);
	$this->register($this);
	return $this;
}


# ------------------------------------------------------------
$COMMANDS->{help} =
  {
   alias => "apropos",
   description => "provide help for a command",
   synopsis => 
   [
    ["pattern"],
    ["-v", undef, "print more help"]
    ],
   routine => \&help,
   };

sub help {
	my ($this, $argv, $opts) = @_;

	foreach my $p ($this->allPlugins) {
		$p->helpCommand($argv, $opts);
	}
	print "\ntry help -v or <command> -h for detailed help.\n";
}

# ----------------------------------------
sub findPluginFile {
	my($filename) = @_;

	my $filename = "$filename.pm" unless ($filename =~ /\.pm$/);
	my($realfilename,$result);
      ITER: {
		foreach my $prefix (@INC) {
			$realfilename = "$prefix/$filename";
			if (-f $realfilename) {
				# print "$realfilename\n";
				last ITER;
			}
		}
		print "Can't find $filename in \@INC.\nINC = @INC\n";
		return undef;
	}
	return $realfilename;
}

# ----------------------------------------
sub load {
	# replaces require, but load each time

	my($filename) = @_;

	my ($result, $realfilename);
	$realfilename = findPluginFile($filename);
	return unless $realfilename;

	$result = do $realfilename;
	die $@ if $@;
	die "$filename did not return true value" unless $result;
	$INC{$filename} = $realfilename;

	#return $result;
	if ($result) {
		return $realfilename
	}
	return undef;
}

# ------------------------------------------------------------
$COMMANDS->{register} = 
  {
   alias => "",
   description => "register a plugin or list registerd plugins",
   synopsis => 
   [
    ["plugin"]
    ],
   routine => \&registerI,
   };

sub registerPluginNamed {
	my ($this, $plugin, $noReload) = @_;

	# we don't want Senora to die on filename typos, hence the evals

	my $filename;
	unless ($noReload) {
		eval {$filename = load "$plugin";};

		print $@;
		return 0 if $@;
		return unless $filename;

	}

	#print "plugin=$plugin::FILE\n";

	my $po = $plugin->new;
	# remember the filename
	$po->{FILE} = "$filename";
	my $pi = $this->register ($po);
	print $@;
	return 0 if $@;

	return $pi;
}

sub registerI {
	# interactively register a plugin
	my ($this, $argv, $opts) = @_;

	if ($this->noArgs(@_)) {
		return $this->listPlugins;
	} else {
		if ($this->registerPluginNamed($argv->[0])) {
			print "$argv->[0] registered\n" 
			  unless ($Senora::OPTS->{"s"});
		}
	}
}

# ------------------------------------------------------------
sub listPlugins {
	my ($this) = @_;
	print "registered Plugins:\n";
	my $i=-1;
	foreach my $plugin ($this->allPlugins) {
		printf ("\t%-16s",ref($plugin));
		$i++;
		if (($i%4)==3) {
			print "\n";
		}
	}
	print "\n";
}
# ------------------------------------------------------------
$COMMANDS->{unregister} = 
  {
   alias => "",
   description => "unregister a plugin",
   synopsis => 
   [
    ["plugin"]
    ],
   routine => \&unregisterI,
   };

sub unregisterI {
	# interactively unregister a plugin
	my ($this, $argv, $opts) = @_;
	my $pluginName = $argv->[0];

	my $plugin = $this->findPlugin($pluginName);
	$this->unregister($plugin) if $plugin;
}

# ------------------------------------------------------------
newSetting("pluginCode","OFF","ON",
	      "always show SQL code before executing")->{alias}="debug";
newSetting("nop","OFF","ON",
	      "don't execute anyting against the database")->{alias}="nop";

# ------------------------------------------------------------
sub register {
	my ($this, $plugin) = @_;

	# unregister if already registered
	if (my $oldPlugin = $this->findPlugin(ref($plugin))) {
		$this->unregister($oldPlugin);
		print ref($plugin) . " unregistered.\n"unless ($Senora::OPTS->{"s"});

	}

	return $this->{plugins}->{$plugin}=$plugin;
}

# ------------------------------------------------------------
sub unregister {
	my ($this, $plugin) = @_;
	delete $this->{plugins}->{$plugin};
}

# ------------------------------------------------------------
sub allPlugins {
	my ($this) = @_;

	return values (%{$this->{plugins}});
}

# ------------------------------------------------------------
sub findPlugin {
	my ($this, $name) = @_;

	foreach my $plugin ($this->allPlugins) {
		if (ref($plugin) eq $name) {
			return $plugin;
		}
	}
	return 0;
}



# ------------------------------------------------------------
sub canRun {
	# find if any plugin that can run cmdline
	# return hash(plugin, comman) if so

	my ($this, $cmdline) = @_;

	my $plugin="one";
	my $cmd;

	# first try official names
	foreach my $plugin ($this->allPlugins) {
		if ($cmd = $plugin->canRunCommand($cmdline)) {
			return {plugin => $plugin, command => $cmd};
		}
	}

	# then try alias names
	foreach my $plugin ($this->allPlugins) {
		if ($cmd = $plugin->canRunAlias($cmdline)) {
			return {plugin => $plugin, command => $cmd};
		}
	}

	# No plugin runs that
	return undef;
}



# ------------------------------------------------------------
sub runPlugin{
	# run $pluginCommand = [package, command]

	my ($this, $pluginCommand, $cmdline) = @_;
	my $plugin = $pluginCommand->[0];
	my $command = $pluginCommand->[1];

	$plugin->runCommand($command, $cmdline);
}


# ------------------------------------------------------------
sub runCommand {
	# find plugin command and run it. Watchout: don't call this like a
	# routine in Plugin. 

	my ($this, $cmdline) = @_;

	# too bad: we must find it again to avoid tramp data
	if (my $match = $this->canRun($cmdline)) {
		$match->{plugin}->runCmdline($match->{command}, $cmdline);
		return 1;
	}

	# if for some reson, someone tries to run a plugin 
	# that is not a plugin:
	print "Don't know how to run '$cmdline'\n";
	return 0;
}



# ------------------------------------------------------------
$COMMANDS->{template} =
  {
   alias => "",
   description => "print a template for a plugin command",
   synopsis => 
   [ [],
    ["-c","command","the name of the command"],
    ["-p","plugin","create a new plugin (be careful !)"],
    ["-f","filename","append to file"],
    ["-a","plugin","append to existing plugin"],
    ["-n","nr","convert history item to new command"]
   ],
   man => qq(
	Template creates a template for a new command, that will
	need some moderate editing. With the -a option you can add
	it to an existing plugin (file). Otherwise it is printed to
	the screen.

	If you have an interesting statement in you history (say as
	item 11) you can turn it into a command by running

	template myNewCommand -n11 -pScratch

	You need to re-register the plugin afterwards.
),
   routine => \&template
   };

sub template {
	my ($this, $argv, $opts) = @_;
	if ($opts->{p} and $opts->{a}) {
		print "You cannot use -p with -a\n";
		return;
	}
	my $commandName = $opts->{c} ? $opts->{c} : "Unnamed";
	my $pluginName = $opts->{p} if $opts->{p};

	my $filename;
	my $fd;

	# append to existing plugin ?
	if (my $p = $opts->{a}) {
		$filename = findPluginFile($p);
		unless ($filename) {
			print "cannot find file of $p\n.";
			return;

		}
	}
	# explicit filename always wins
	if ($opts->{f}) {
		$filename = $opts->{f};
	}
	# must start with uppercase
	$filename = initcap($filename);
	$pluginName = initcap($pluginName);

	if ($filename and $pluginName and $filename !~ "$pluginName.pm") {
		print "filename should be $pluginName.pm\n";
		return;
	}

	if ($filename) {
		unless ($fd = FileHandle->new(">>$filename")) {
			print "cannot open $filename\n.";
			return;
		}
	}

	my $sqlCommand;
	if ($opts->{n}) {
		my $cmd = $Senora::HISTORY->getItemNumbered($opts->{n});
		return unless $cmd;
		my $command = $cmd->get;
		$sqlCommand = qq(sql->run(qq($command\t)));
	}

	my $pluginTemplate = qq(

\=head1 NAME

	$pluginName - xxx what plugin $pluginName does

\=head1 RESPONSIBILITIES
	xxx describe $pluginName\'s responsibilities here

\=head1 COLLABORATIONS 
	xxx describe collablorations here
=cut 

# \$Id\$

package $pluginName;
use strict;
use Settings;
use Plugin;
use SessionMgr qw(sessionNr sql);
use Misc;

use vars qw 
  (
   \@ISA
   \$COMMANDS
);

\@ISA = qw (
	   Plugin
);
#------------------------------------------------------------
sub new {
	my (\$That) = \@_;
	my \$class = ref(\$That) || \$That;
	my \$this = {};

	\$this->{commands} = \$COMMANDS;
	bless (\$this, \$class);
	return \$this;
}
);

	my $commandTemplate = qq(
# ------------------------------------------------------------
\$COMMANDS->{$commandName} =
  {
   alias => "",
   description => "+put one line description here",
   man=>q(
	Put a more verbose description here. Indent by one tab.
	Can be omitted entirely.
BUGS
	Mention known bugs here
),
   synopsis => 
   [
    ["remainingArg"],
    ["-a",undef,"comment here"], 	#option without args
    ["-b","argName","comment here"],	#option with an argument
    ],
   routine => \\&$commandName,
   };

sub $commandName {
	my (\$this, \$argv, \$opts) = \@_;
	print "$commandName called\n";
	$sqlCommand;
}
);
if ($fd) {
	print $fd $pluginTemplate if $opts->{p};
	print $fd $commandTemplate;
	$fd->close if $fd
}else {
	print $pluginTemplate if $opts->{p};
	print $commandTemplate;
}

}
1;
