
=head1 NAME

ConIo - Console input and output

=head1  RESPONSIBILITIES

Provide read/write method that do not use stdin/stdout. Make
this work under Windows too. Make output unbuffered.

=head1 COLLABORATIONS

This is basically needed for ampersand replacing in MainLoop

=cut

# $Id: ConIo.pm,v 1.1 2011/03/25 14:36:23 ekicmust Exp $

package ConIo;
use strict;
use Exporter;
use FileHandle;
use vars qw (
	     @ISA
	     @EXPORT
	     $IN
	     $OUT
	     );

@ISA = qw (
	   Exporter
);

@EXPORT=qw(
	   conOut
	   conIn
);

# always open console at startup

if ($^O =~ /win/i) {
	#print "We're on windows.\n";
	$IN = FileHandle->new('CON') ;
	$OUT = FileHandle->new('>CON') ;
} else {
	$IN = FileHandle->new("/dev/tty") ;
	$OUT = FileHandle->new(">/dev/tty") ;
}

if (! $IN or ! $OUT) { die "Cannot open console";}

# ----------------------------------------
sub conOut {
	my ($text) = @_;
	print $OUT $text;
	$OUT->flush;
}

# ----------------------------------------
sub conIn {
	my ($text) = @_;
	return (<$IN>);
}

1;
