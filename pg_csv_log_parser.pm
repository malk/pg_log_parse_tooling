#!/usr/bin/perl
package pg_csv_log_parser;

use warnings;
use strict;
use Text::CSV;

use vars qw($VERSION @ISA @EXPORT);

$VERSION = '0.01';

require Exporter;

@ISA = qw(Exporter);
@EXPORT =
	qw(
		  LOG_TIME
		  USER_NAME
		  DATABASE_NAME
		  PROCESS_ID
		  CONNECTION_FROM
		  SESSION_ID
		  SESSION_LINE_NUM
		  COMMAND_TAG
		  SESSION_START_TIME
		  VIRTUAL_TRANSACTION_ID
		  TRANSACTION_ID
		  ERROR_SEVERITY
		  SQL_STATE_CODE
		  MESSAGE
		  DETAIL
		  HINT
		  INTERNAL_QUERY
		  INTERNAL_QUERY_POS
		  CONTEXT
		  QUERY
		  QUERY_POS
		  LOCATION
		  fix
		  iterate
		  tail
		  shifter
		  apply
		  collect
		  filter
		  head_cut
		  stopgag
		  reduce
		  count_if
		  multi_reduce
		  filter_query
		  linear_open
		  input
		  output
		  collect_all
		  fields_with_queries
		  queries
		  concat_of_queries
		  extract_tables_from_log_entry
		  extract_tables_from_query
		  I
		  TK
	 );


use constant {
	LOG_TIME => 0,
	USER_NAME => 1,
	DATABASE_NAME => 2,
	PROCESS_ID => 3,
	CONNECTION_FROM => 4,
	SESSION_ID => 5,
	SESSION_LINE_NUM => 6,
	COMMAND_TAG => 7,
	SESSION_START_TIME => 8,
	VIRTUAL_TRANSACTION_ID => 9,
	TRANSACTION_ID => 10,
	ERROR_SEVERITY => 11,
	SQL_STATE_CODE => 12,
	MESSAGE => 13,
	DETAIL => 14,
	HINT => 15,
	INTERNAL_QUERY => 15,
	INTERNAL_QUERY_POS => 17,
	CONTEXT => 18,
	QUERY => 19,
	QUERY_POS => 20,
	LOCATION => 21
};

# the I (id or 'idiot bird') combinator
sub I {
	return sub {
		return shift;
	};
}

# The TK (Thrush-Kestrel) combinator, applies reversed.
sub TK($$) {
	my ($y, $x, $tmp) = @_;
	return sub {
		$tmp = $x->();
		$y->($tmp);
		return $tmp;
	};
}

sub fix($) {
	my ($sequence, $last, $entry) = @_;
	$last = $sequence->();
	return undef unless $last;
	do {
		$entry = $sequence->();
		return undef unless $entry;
		return $entry if $last == $entry;
		$last = $entry;
	} while ($entry);
}

# calls a closure until it returns false (to iterate over the log entries, the "main loop")
sub iterate($) {
	my ($sequence, $entry) = @_;
	do {
		$entry = $sequence->();
	} while ($entry);
}

sub tail($) {
	my ($sequence, $entry, $tail) = @_;
	$entry = $sequence->();
	return undef unless $entry;
	do {
		$tail = $entry;
		$entry = $sequence->();
	} while ($entry);
	return $tail;
}

# a test closure, that just empties an array passed as argument sesuentially
sub shifter(@) {
	my @array = @_;
	return sub {
		return shift @array;
	};
}

sub apply($$) {
	my ($closure, $entry) = @_;
	return $entry ? $closure->($entry) : undef;
}

# applies a closure to another
sub collect($$) {
	my ($collector, $sequence) = @_;
	return sub {
		return apply $collector, $sequence->();
	};
}



# only return log entries that respect the predicate
sub filter($$) {
	my ($predicate, $sequence, $entry) = @_;
	return sub {
		do {
			$entry = $sequence->();
		} while ($entry && !$predicate->($entry));
		return $entry;
	};
}

# ignores all entries until predicate is true, freely pass things around after thatof the sequence
sub head_cut($$) {
	my ($predicate, $sequence, $entry, $keep_searching) = @_;
	$keep_searching = 1;
	return sub {
		$entry = $sequence->();
		return $entry unless $keep_searching && $entry;
		$entry = $sequence->() until !$entry || $predicate->($entry);
		$keep_searching = undef;
		return $entry;
	}
}

# if predicate is true, stops iterating consider it the end of the sequence
sub stopgag($$) {
	my ($predicate, $sequence, $entry) = @_;
	return sub {
		$entry = $sequence->();
		return undef unless $entry;
		if ($predicate->($entry)) {
			$sequence = sub {return undef;};
			return undef;
		}
		return $entry;
	}
}

sub reduce($$$) {
	my ($reducer, $current, $sequence, $entry, $last) = @_;
	return sub {
		$entry = $sequence->();
		unless($entry) {
			my $last = $current;
			$current = undef;
			return $last;
		}
		return $current = $reducer->($current, $entry);
	}
}

# counting ocurrences of a predicate is a common usage of reduce
sub count_if {
	my $predicate = shift;
	return sub {
		my ($current, $entry) = @_;
		return $current + ($predicate->($entry));
	}
}

# combine log entries into a set of results
sub multi_reduce($$$) {
	my ($reducer, $current, $sequence, $entry, $size, $i) = @_;
	$size = @$current;
	return sub {
		$entry = $sequence->();
		unless ($entry) {
			my $last = $current;
			$current = undef;
			return $last;
		}

		for ($i = 0; $i < $size; $i++) {
			$current->[$i] = $reducer->[$i]->($current->[$i], $entry)
		}
		return $current;
	}
}

sub extract_tables_from_query {
	return sub {
		my $query = shift;
		my @result = ($query =~ /\sFROM\s+(\w+)/ig);
		@result = (@result, ($query =~ /\sJOIN\s+(\w+)/ig));
		@result = (@result, ($query =~ /\sINTO\s+(\w+)/ig));
		@result = (@result, ($query =~ /UPDATE\s+(\w+)/ig));
		my %seen = ();
		foreach my $table (@result) {
			$seen{$table}++;
		}
		@result = keys %seen;
		return @result;
	};
}

sub fields_with_queries() {
	return (QUERY, INTERNAL_QUERY, MESSAGE, CONTEXT);
}

sub queries($) {my $x = shift;
	     my @result = ();
	     if ($x) {
		     foreach my $field (fields_with_queries) {
			     @result = (@result, $x->[$field]) if $x->[$field];
		     }
	     }
	     return @result;
}

sub concat_of_queries($) {
	return join ' ', queries shift;
}

sub extract_tables_from_log_entry {
	return sub {
		return extract_tables_from_query
			concat_of_queries shift;
	};
}

sub filter_log_entry($) {
	my $entry = shift;
	return undef unless $entry;

	foreach my $field (fields_with_queries) {
		$entry->[$field] =~ tr/\r\n/  / if $entry->[$field];
	}

	return $entry;
}

sub linear_open {
	my ($mode, $default_handle, $file, $handle) = @_;
	$handle = $default_handle unless $file && open $handle, $mode, $file;
	return $handle;
}

sub input {
	my ($file, $handle, $csv) =
	(shift || $ARGV[0], undef, Text::CSV->new({ binary => 1, eol => $/ }));

	$handle = linear_open '<', *STDIN, $file;

	return sub {
		return filter_log_entry $csv->getline($handle);
	};
}

sub output {
	my ($file, $csv, $handle, $entry, $print_csv) =
	(shift || $ARGV[1], Text::CSV->new({ binary => 1, eol => $/ }));

	$handle = linear_open '>', *STDOUT, $file;

	$print_csv = sub { $csv->print($handle, shift); };

	return sub {
		return apply $print_csv, shift;
	}
}

sub collect_all {
	my ($collector, $input_file, $output_file) = @_;
	my $input = input $input_file;
	my $output = output $output_file;

	my $output_collected = sub {
		return $output->($collector->(shift));
	};

	iterate collect $output_collected, $input;
}

1;
