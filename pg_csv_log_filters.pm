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
# Copyright 2012 Romeu Moura
# This file is part of pg_log_parse_tooling. pg_Log_Parse_Tooling is
# free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your
# option) any later version. pg_Log_Parse_Tooling is distributed in
# the hope that it will be useful, but WITHOUT ANY WARRANTY; without
# even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the GNU General Public License for more
# details. You should have received a copy of the GNU General Public
# License along with pg_Log_Parse_Tooling. If not, see
# <http://www.gnu.org/licenses/>.
