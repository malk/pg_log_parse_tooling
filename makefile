.PHONY: pg_csv_log_parser

pg_csv_log_parser: pg_csv_log_parser.pm
	(eval $$(perl -I ~/perl5/lib/perl5/ -Mlocal::lib); ./pg_csv_log_parser.pm)
