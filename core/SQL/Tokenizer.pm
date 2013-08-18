package SQL::Tokenizer;

use warnings;
use strict;

use 5.006002;

use Exporter;

our @ISA = qw(Exporter);

our @EXPORT_OK= qw(tokenize_sql);

our $VERSION= '0.19';

my $re= qr{
    (
        (?:--|\#)[\ \t\S]*      # single line comments
        |
        (?:<>|<=>|>=|<=|==|=|!=|!|<<|>>|<|>|\|\||\||&&|&|-|\+|\*(?!/)|/(?!\*)|\%|~|\^|\?)
                                # operators and tests
        |
        [\[\]\(\),;.]            # punctuation (parenthesis, comma)
        |
        \'\'(?!\')              # empty single quoted string
        |
        \"\"(?!\"")             # empty double quoted string
        |
        ".*?(?:(?:""){1,}"|(?<!["\\])"(?!")|\\"{2})
                                # anything inside double quotes, ungreedy
		|
        `.*?(?:(?:``){1,}`|(?<![`\\])`(?!`)|\\`{2})
                                # anything inside backticks quotes, ungreedy
        |
        '.*?(?:(?:''){1,}'|(?<!['\\])'(?!')|\\'{2})
                                # anything inside single quotes, ungreedy.
        |
        /\*[\ \t\n\S]*?\*/      # C style comments
        |
        (?:[\w:@]+(?:\.(?:\w+|\*)?)*)
                                # words, standard named placeholders, db.table.*, db.*
        |
        \n                      # newline
        |
        [\t\ ]+                 # any kind of white spaces
    )
}smx;

sub tokenize_sql {
    my ( $query, $remove_white_tokens )= @_;

    my @query= $query =~ m{$re}smxg;

    if ($remove_white_tokens) {
        @query= grep( !/^[\s\n\r]*$/, @query );
    }

    return wantarray ? @query : \@query;
}

sub tokenize {
    my $class= shift;
    return tokenize_sql(@_);
}

1;

=pod

=head1 NAME

SQL::Tokenizer - A simple SQL tokenizer.

=head1 VERSION

=head1 SYNOPSIS

 use SQL::Tokenizer qw(tokenize_sql);

 my $query= q{SELECT 1 + 1};
 my @tokens= SQL::Tokenizer->tokenize($query);

 # @tokens now contains ('SELECT', ' ', '1', ' ', '+', ' ', '1')

 @tokens= tokenize_sql($query); # procedural interface

=head1 DESCRIPTION

SQL::Tokenizer is a simple tokenizer for SQL queries. It does not claim to be
a parser or query verifier. It just creates sane tokens from a valid SQL
query.

It supports SQL with comments like:

 -- This query is used to insert a message into
 -- logs table
 INSERT INTO log (application, message) VALUES (?, ?)

Also supports C<''>, C<""> and C<\'> escaping methods, so tokenizing queries
like the one below should not be a problem:

 INSERT INTO log (application, message)
 VALUES ('myapp', 'Hey, this is a ''single quoted string''!')

=head1 API

=over 4

=item tokenize_sql

 use SQL::Tokenizer qw(tokenize_sql);

 my @tokens= tokenize_sql($query);
 my $tokens= tokenize_sql($query);

 $tokens= tokenize_sql( $query, $remove_white_tokens );

C<tokenize_sql> can be imported to current namespace on request. It receives a
SQL query, and returns an array of tokens if called in list context, or an 
arrayref if called in scalar context.

=item tokenize

 my @tokens= SQL::Tokenizer->tokenize($query);
 my $tokens= SQL::Tokenizer->tokenize($query);

 $tokens= SQL::Tokenizer->tokenize( $query, $remove_white_tokens );

This is the only available class method. It receives a SQL query, and returns an
array of tokens if called in list context, or an arrayref if called in scalar
context.

If C<$remove_white_tokens> is true, white spaces only tokens will be removed from
result.

=back

=head1 ACKNOWLEDGEMENTS

=over 4

=item 

Evan Harris, for implementing Shell comment style and SQL operators.

=item

Charlie Hills, for spotting a lot of important issues I haven't thought.

=item

Jonas Kramer, for fixing MySQL quoted strings and treating dot as punctuation character correctly.

=back

=head1 AUTHOR

Copyright (c) 2007, 2008 Igor Sutton Lopes "<IZUT@cpan.org>". All rights
reserved.

This module is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut

