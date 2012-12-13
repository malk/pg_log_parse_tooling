#!/usr/bin/perl
use warnings;
use strict;
use pg_csv_log_parser;
use pg_csv_log_filters;

my @patterns = map { qr/$_/ }
	qw(fco_ groupe utilisateur formulaire piece_jointe);

my $has_commmunication_tables = has_table \@patterns;

iterate collect output, filter $has_commmunication_tables, input;


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
