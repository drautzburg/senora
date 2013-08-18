package Setting;

use strict  qw(vars subs);
use vars qw (@ISA);


# ------------------------------------------------------------
sub new {
	my ($this) = @_;
	return bless {};
}

# ------------------------------------------------------------
sub copy {
	my ($this) = @_;
	my $copy = {};
	foreach my $key (keys(%$this)) {
		$copy->{$key} = $this->{$key}
	}
	return bless $copy;
}

# ------------------------------------------------------------
sub get {
	my ($this, $key) = @_;
	return $this->{uc($key)};
}

# ------------------------------------------------------------
sub set {
	my ($this, $key, $value) = @_;
	# print "set $key $value\n";
	# unquote
	$value =~ s/^"//;
	$value =~ s/"$//;
	$this->{uc($key)} = $value;

}

# ------------------------------------------------------------
sub setInitially {
	# set unless already set
	my ($this, $key, $value) = @_;
	return if ($this->get($key));
	$this->{$key} = $value;

}

# ------------------------------------------------------------
sub printOne {
	my ($this, $key) = @_;
	my $value = $this->get($key);
	printf ("%-12s: %s\n", $key, $value ? $value : "undefined");
}

# ------------------------------------------------------------
sub printAll {
	my ($this) = @_;
	foreach my $key (keys(%$this)) {
		$this->printOne($key)
	}
}



1;
