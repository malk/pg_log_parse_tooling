.PHONY: pg_csv_log_parser

pg_csv_log_parser: pg_csv_log_parser_tests.pm
	(eval $$(perl -I ~/perl5/lib/perl5/ -Mlocal::lib); ./pg_csv_log_parser_tests.pm)

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
