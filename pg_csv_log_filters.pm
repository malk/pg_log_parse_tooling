#!/usr/bin/perl
package pg_csv_log_filters;

use warnings;
use strict;

use pg_csv_log_parser;
use vars qw($VERSION @ISA @EXPORT);

$VERSION = '0.01';

require Exporter;

@ISA = qw(Exporter);
@EXPORT =
	qw(
		  field_has
		  message_has
		  is_timeout
		  is_unexpected_EOF
		  is_syntax_error
		  has_table
		  extract_tables
	 );

sub new{
	my $self = {};
	bless $self;
	$self->init;
	return $self;
}

sub field_has($$) {
	my ($field, $message) = @_;
	my $pattern = qr/$message/;
	return sub {
		my $x = shift;
		return $x->[$field] && $x->[$field] =~ $pattern;
	};
}

sub message_has($) {
	return field_has MESSAGE, shift;
}

sub is_timeout() {
	return message_has 'imeout';
}

sub is_unexpected_EOF() {
	return message_has 'unexpected EOF';
}

sub is_syntax_error() {
	return message_has 'syntax error';
}

sub has_table {
	my $patterns = shift;
	my @fields = fields_with_queries;
	return sub {
		my $x = shift;
		foreach my $field (@fields) {
			if ($x->[$field]) {
				foreach my $pattern (@$patterns) {
					return 1 if $x->[$field] =~ $pattern;
				}
			}
		}
		return 0;
	};
}

1;
