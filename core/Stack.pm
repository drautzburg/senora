=head1 NAME

Stack - maintain a stack of anything

= head1 RESPONSIBILITIES

Provide LIFO and FIFO mechanism with readable function names.

=head1 COLLABORATIONS

Inspired by TOMC at CPAN

=cut

# $Id: Stack.pm,v 1.1 2011/03/25 14:36:25 ekicmust Exp $

package Stack;
use strict;
use Carp;

# ----------------------------------------
sub new {
	my ($this, $max) = @_;
	my $class = ref($this) || $this;
	my $this = {
		     maxsize => $max ? $max : 100,
		     arr =>[],
		     };
	return bless($this, $class);
}

#----------------------------------------
sub size {
	my $this = shift;
	return scalar @{$this->{arr}};
} 

#----------------------------------------
sub is_empty {
	my $this = shift;
	return 0 == scalar @{$this->{arr}};
} 

#----------------------------------------
sub push {
	my $this = shift;
	push(@{$this->{arr}}, @_);
	while ($this->size > $this->{maxsize}) {
		shift @{$this->{arr}};
	}
} 

#----------------------------------------
sub pop {
	my $this = shift;
 	confess("Stack is empty!") if $this->is_empty;
 	return pop (@{$this->{arr}});
}

#----------------------------------------
sub peek {
	# index=1 least recently added, index=2 the one before

	my ($this,$index) = @_;;
	if ($this->is_empty) {
		print ("Stack empty!\n");
		exit;
	}
	return $this->{arr}-> [$this->size - $index]
}

#----------------------------------------
sub top {
	my ($this) = @_;
	return $this->peek(1);
}


#----------------------------------------
sub asArray {
	my ($this) = @_;
	return @{$this->{arr}};
}
#----------------------------------------
sub dump {
	# print stack contents

	my ($this) = @_;
	for (my $i = $this->size; $i>=0; $i--) {
		print $this->peek ($i) . "\n";
	}
}

1;

