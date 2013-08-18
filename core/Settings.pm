=head1 NAME

Settings - Manage several sets of global variables

=head1 RESPONSIBILITIES

Provide functions to set and recall settings. Settings are grouped by
'columnFormats', 'defines' etc. Provide convenience function to define
commands to get and set settings. Provide a way to push and pop
setings to and from a Stack. The latter is useful when a script calls
another script.

=head1 COLLABORATIONS

Setting manages ONE set of variables.
Settings IS A Stack.

=cut


package Settings;


use strict  qw(vars subs);

use Stack;
use Setting;
use Exporter;
use vars qw(
	@ISA
	@EXPORT
);

@ISA = qw(Stack Exporter);
@EXPORT = qw (settings setting define columnFormat setOn newSetting);

my $Settings;
# ------------------------------------------------------------
sub new {
	my ($this) = @_;
	my $s = Stack->new;
	$s->push ({
		   settings => Setting->new,
		   defines => Setting->new,
		   columnFormats => Setting->new
	});
	return bless $s;
}

# ------------------------------------------------------------
sub settings {
	return $Settings;
}
# ------------------------------------------------------------
sub setting {
	# exported shortcut
	return $Settings->top->{settings};
}

# ------------------------------------------------------------
sub setOn {
	my($name) = @_;
	#print "setOn $name " . setting->get($name) ."\n";

	return 1 if setting->get($name) =~ /(ON|TRUE)/i;
	return 0;
}
# ------------------------------------------------------------
sub define {
	# exported shortcut
	return $Settings->top->{defines};
}
# ------------------------------------------------------------
sub columnFormat {
	# exported shortcut
	return $Settings->top->{columnFormats};
}

# ------------------------------------------------------------
sub push {
	my ($this) = @_;

	my $copy = {
		    settings => setting->copy,
		    defines => define->copy,
		    columnFormats => columnFormat->copy
		   };
	$this->SUPER::push($copy);
}

# ------------------------------------------------------------
sub pop {
	my ($this) = @_;
	if ($this->size <=1) {
		print "Nothing to pop.\n";
		return;
	}
	$this->SUPER::pop;
}


# ------------------------------------------------------------
sub genericCommand{
	# generic command to set a 'setting'

	my ($name, $default, $others, $argv, $opts) = @_;
	my $value = $argv->[0];

	if ($others eq "aNumber" && $value =~ /^[0-9]+$/) {
		setting->set($name, $value);
		return;
	}
	if ($others =~ "aChar" && $value =~ /^.$/) {
		setting->set($name, $value);
		return;
	}

	elsif (uc($value) =~ /^($default|$others)$/) {
		setting->set($name, uc($value));
		return;
	}else {
		# no valid value: show current setting
		setting->printOne($name);
	}
}

# ------------------------------------------------------------
sub newSetting {
	my ($name, $default, $others, $descr) = @_;

	# get the COMMANDS hash from the callers namespace
	my $v = (caller)."::COMMANDS";
	my $CMDS=$$v;
	$descr = "get/set $name" unless $descr;
	# create a command entry
	my $cmdEntry = {
			description => "$descr",
			synopsis => [["$default|$others"]],
			routine => sub {
				my ($that, $argv, $opts) = @_;
				genericCommand (
						       $name, 
						       $default, 
						       $others, 
						       $argv, 
						       $name)
				}
		       };
	$CMDS->{"set  *$name"} = $cmdEntry;

	# set the default value
	setting()->set($name, $default);
	return $cmdEntry;
}




# ------------------------------------------------------------
sub printAll {
	my ($this) = @_;
	foreach my $stackFrame ($Settings->asArray()) {
		print "printing " . $this->size() . " stack frames\n";
		print "==============================================\n";
		print "--- settings --- \n";
		$stackFrame->{settings}->printAll();
		print "--- defines --- \n";
		$stackFrame->{defines}->printAll();
		print "--- columnFormats --- \n";
		$stackFrame->{columnFormats}->printAll();
	}
}
# ------------------------------------------------------------
# Singleton instance
$Settings = Settings->new;


1;
