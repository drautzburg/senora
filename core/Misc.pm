=head1 NAME

	Misc - provide some helpers

=head1 RESPONSIBILITIES

=head1 COLLABORATIONS

=cut

# $Id: Misc.pm,v 1.1 2011/03/25 14:36:23 ekicmust Exp $

package Misc;
use strict;
use Exporter;
use vars qw (
	     @EXPORT
	     @ISA
);
@ISA = qw (Exporter);
@EXPORT=qw(
	   access
	   initcap
	   max
	   min
	   qnu
	   glob2ora
	   processPattern
	   really
	   megaBytes
	   commify
);
# ------------------------------------------------------------
sub new {
	my ($Misc) = @_;
	my $class = ref($Misc) || $Misc;
	my $this={
		  };
	bless ($this, $class);
	return $this;
}

# ------------------------------------------------------------
sub qnu {
   	# qouote string is not entirely uppercase
	my ($string) = @_;

	if ($string =~ /^[\.\(\)0-9A-Z_]*$/) {
		return $string;
	}
	return "\"$string\"";
}
# ------------------------------------------------------------
sub initcap {
	# uppercase character after stop characters
	# do nothing to quoted strings

	my ($string) = @_;
	return unless $string;
	# do nothing on quoted strings
	if ($string =~ /".*"/) {
		return $string;
	}
	$string = lc($string);
	$string =~ s/^(.)(.*)/uc($1).$2/e;
	$string =~ s,([ _\$/])([a-z]),$1.uc($2),eg;
	return $string;
}

# ------------------------------------------------------------
sub allcap {
	# uppercase everything
	# do nothing to quoted strings

	my ($string) = @_;
	return unless $string;
	# do nothing on quoted strings
	if ($string =~ /".*"/) {
		return $string;
	}
	return uc($string);
}


# ----------------------------------------
sub max {
	# return the maximum of any number of values
	my $max = -1;
	foreach my $i (@_) {
		if ($i > $max) {$max = $i}
	}
	return $max;
}
# ----------------------------------------
sub min {
	# return the minimum of any number of values
	my $min = 10000000000;
	foreach my $i (@_) {
		if ($i < $min) {$min = $i}
	}
	return $min;
}

# ------------------------------------------------------------
sub access {
	# generic get/set routine
	my ($field) = @_;
	my $class = (caller(0))[0];
	my $routine = qq
	  (
	   sub $class\:\:$field {
		       my (\$this, \$value) = \@_;
		       #print "\$this \$field \$value \\n";
		       if (\$value) {
			       \$this->{\$field} = \$value;
		       }
		       return \$this->{\$field};
		       }
	   );
	   #print "$routine";
	   eval "$routine";
}

#------------------------------------------------------------
sub glob2ora {
	my ($glob) = @_;
	my $ora = $glob;
	# replace "*" by "%"
	$ora=~ s/\*/\%/g; 
	# uppercase and in single quotes
	return "'".unquote(allcap($ora))."'";
}
#------------------------------------------------------------
sub processPattern {
	# return (typePattern, namePattern) 
	my ($argv) = @_;

	my $pattern;
	if ($argv->[1]) { # "package body"
		$pattern = $argv->[0] . " " . $argv->[1];
	} else {
		$pattern = $argv->[0];
	}

	my ($typePattern, $namePattern) = split("/",$pattern);

	# if there is just one pattern, we take it as namePattern ("ls foo")
	if (!$namePattern) {
		$namePattern = $typePattern ? $typePattern : "%";
		$typePattern = "%";
	}

	return (glob2ora($typePattern), glob2ora($namePattern));
}

#------------------------------------------------------------
sub unquote {
	my ($string) = @_;
	$string =~ s/"//g;
	return $string;
}
#------------------------------------------------------------
sub sql_obsoltet {
	# shorthand command to get the current session
	# xxx this place is legay. It just redirects to:
	return SessionMgr::sql();
}



#------------------------------------------------------------
sub commify {
        local $_  = shift;
        1 while s/^([-+]?\d+)(\d{3})/$1,$2/;
        return $_;
}

#------------------------------------------------------------
sub really {
	# return 0 if not really

	my ($question) = @_;
	ConIo::conOut("really ".$question);
	my $answer = ConIo::conIn();
	if (uc($answer) =~ /Y(ES)*/) {
		return 1;
	}

	return 0;
}
#------------------------------------------------------------
sub megaBytes {
	return sprintf ("%4.1f MB", $_[0]/(1024*1024));
}
