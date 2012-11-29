#!/usr/bin/perl
use warnings;
use strict;
use pg_csv_log_parser;
use pg_csv_log_filters;

my @patterns = map { qr/$_/ }
	qw(fco_ groupe utilisateur formulaire piece_jointe);

my $has_commmunication_tables = has_table \@patterns;

iterate collect output, filter $has_commmunication_tables, input;


