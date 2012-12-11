#!/usr/bin/perl
package pg_csv_log_parser;

use warnings;
use strict;
use Text::CSV;
use Test::Extreme;
use SQL::Statement;

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
		  query_parser
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

sub test_fix {
	my @entries = (1, 2, 1);
	my @expected = ();
	my $shifter = sub { return shift @entries; };
	fix $shifter;
	eval { assert_equals @expected, @entries } ; assert_passed;
}

# calls a closure until it returns false (to iterate over the log entries, the "main loop")
sub iterate($) {
	my ($sequence, $entry) = @_;
	do {
		$entry = $sequence->();
	} while ($entry);
}

sub test_iterate {
	my @entries = (1, 2, 1);
	my @expected = ();
	my $shifter = sub { return shift @entries; };
	iterate $shifter;
	eval { assert_equals @expected, @entries } ; assert_passed;
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

sub test_tail {
	eval { assert_equals 1, tail shifter(3, 2, 1) } ; assert_passed;
}

sub assert_equals_sequences($$) {
	my ($first, $second, $i, $j) = @_;
	do {
		$i = $first->();
		$j = $second->();
		if ($i || $j) {
			eval { assert $i } ; assert_passed;
			eval { assert $j } ; assert_passed;
			eval { assert_equals $i, $j } ; assert_passed;
		}
	} while ($i && $j);
}

# a test closure, that just empties an array passed as argument sesuentially
sub shifter(@) {
	my @array = @_;
	return sub {
		return shift @array;
	};
}

sub test_assert_equals_sequences {
	eval { assert_equals_sequences shifter(1, 2, 1), shifter(1, 2, 1); } ; assert_passed;
	eval { assert_equals_sequences shifter(1, 2, 1), shifter(2, 1, 2); } ; assert_failed;
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

sub test_collect {
	my $increment = sub { return 1 + shift; };
	eval { assert_equals_sequences shifter(2, 3, 2), collect $increment, shifter(1, 2, 1); } ; assert_passed;
	eval { assert_equals_sequences shifter(2, 3, 2), collect $increment, shifter(2, 1, 2); } ; assert_failed;
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

sub test_filter {
	my $predicate = sub {return 0;};
	eval { assert_equals_sequences shifter(), filter $predicate, shifter(1, 2, 3, 4, 5, 6); } ; assert_passed;
	$predicate = sub {return shift(@_) % 2;};
	eval { assert_equals_sequences shifter(1, 3, 5), filter $predicate, shifter(1, 2, 3, 4, 5, 6); } ; assert_passed;
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

sub test_head_cut {
	my $predicate = sub {return 4 == shift;};
	eval { assert_equals_sequences shifter(4, 5, 6), head_cut $predicate, shifter(1, 2, 3, 4, 5, 6); } ; assert_passed;
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

sub test_stopgag {
	my $predicate = sub {return 4 == shift;};
	eval { assert_equals_sequences shifter(1, 2, 3), stopgag $predicate, shifter(1, 2, 3, 4, 5, 6); } ; assert_passed;
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

sub test_reduce {
	my $adder = sub {return shift(@_) + shift(@_)};
	eval { assert_equals 6, fix reduce $adder, 0, shifter(1, 2, 3); } ; assert_passed;
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

sub test_multi_reduce {
	my $adder = sub {return shift(@_) + shift(@_)};
	my $counter = sub {return shift(@_) + 1};
	my @reducers = ($adder, $counter);
	my @initial = (0, 0);
	my $result = tail multi_reduce \@reducers, \@initial, shifter(1, 2, 3);
	my $actual = shifter @$result;
	eval { assert_equals_sequences shifter(6, 3), $actual} ; assert_passed;
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

sub test_extract_table_from_query {
	my $extract_tables = extract_tables_from_query;

	my @actual = $extract_tables->('SELECT aaaa FROM bbbb');
	my @expected = ('bbbb');
	eval { assert_equals_sequences shifter(@actual), shifter(@expected) } ; assert_passed;

	@actual = $extract_tables->('SELECT count(1) (SELECT aaaa FROM bbbb)');
	@expected = ('bbbb');
	eval { assert_equals_sequences shifter(@actual), shifter(@expected) } ; assert_passed;

	@actual = $extract_tables->('SELECT aaaa FROM cccc WHERE aaaa IN (SELECT aaaa FROM bbbb)');
	@expected = ('bbbb', 'cccc');
	eval { assert_equals_sequences shifter(@actual), shifter(@expected) } ; assert_passed;

	@actual = $extract_tables->('UPDATE fco_fichier SET fco_fic_contenu = $1 WHERE fco_fic_id = $2');
	@expected = ('fco_fichier');
	eval { assert_equals_sequences shifter(@actual), shifter(@expected) } ; assert_passed;

	@actual = $extract_tables->('"SELECT	   fiche.fco_fch_id, fiche.fco_efi_id_initial, fiche.fco_efi_id_courant, fiche.fco_cat_code, fiche.fco_sous_cat_code, fiche.fco_pri_code, fiche.fco_fch_titre, fiche.fco_fch_da, fiche.fco_fch_ej, fiche.fco_fch_dp, fiche.fco_fch_tiers, fiche.fco_fch_sf, fiche.fco_fch_centre_financier, fiche.fco_fch_numero_facture, fiche.fco_fch_oa,  etat_initial.fco_efi_date AS fco_efi_date_creation,  etat_courant.fco_efi_date AS fco_efi_date,  grp_initial.grp_libelle AS grp_initiateur_libelle,  initiateur.uti_prenom AS uti_initiateur_prenom,  initiateur.uti_nom AS uti_initiateur_nom,  etat_courant.fco_efi_code_etat AS fco_efi_code_etat,  categorie.fco_cat_libelle AS fco_cat_libelle,  categorie.fco_sous_cat_libelle AS fco_sous_cat_libelle,  priorite.fco_pri_libelle AS fco_pri_libelle FROM	fco_fiche fiche INNER JOIN fco_etat_fiche etat_courant ON etat_courant.fco_efi_id = fiche.fco_efi_id_courant  OUTER LEFT JOIN fco_priorite priorite ON priorite.fco_pri_code = fiche.fco_pri_code  INNER JOIN fco_categorie categorie ON categorie.fco_cat_code = fiche.fco_cat_code AND categorie.fco_sous_cat_code = fiche.fco_sous_cat_code  INNER JOIN fco_etat_fiche etat_initial ON etat_initial.fco_efi_id = fiche.fco_efi_id_initial  INNER JOIN groupe grp_initial ON grp_initial.grp_id = etat_initial.grp_id_auteur  INNER JOIN utilisateur initiateur ON initiateur.uti_id = etat_initial.uti_id_auteur WHERE 	etat_courant.uti_id_dest = $1  OR (   etat_courant.uti_id_dest IS NULL  AND etat_courant.grp_id_dest IN ($2, $3) ) ORDER BY fiche.fco_fch_id ASC OFFSET $4 LIMIT $5 "');
	@expected = ('groupe', 'utilisateur', 'fco_etat_fiche', 'fco_fiche' , 'fco_priorite', 'fco_categorie');
	eval { assert_equals_sequences shifter(@actual), shifter(@expected) } ; assert_passed;
}

sub query_parser() {
	my $parser = SQL::Parser->new();
	return sub {
		my $query = shift;
		return $query ? SQL::Statement->new($query, $parser) : undef;
	};
}

sub test_query_parser {
	my @expected = ('matab', 'maothertab');
	my $parser = query_parser;
	my $query = $parser->('SELECT field FROM matab JOIN maothertab WHERE fff=1');
	my @actual = map {$_->name} $query->tables();
	eval { assert_equals(@expected, @actual);} ; assert_passed;
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

### Fixtures

my $fixture_query = q{"SELECT
	   fiche.fco_fch_id, fiche.fco_efi_id_initial, fiche.fco_efi_id_courant, fiche.fco_cat_code, fiche.fco_sous_cat_code, fiche.fco_pri_code, fiche.fco_fch_titre, fiche.fco_fch_da, fiche.fco_fch_ej, fiche.fco_fch_dp, fiche.fco_fch_tiers, fiche.fco_fch_sf, fiche.fco_fch_centre_financier, fiche.fco_fch_numero_facture, fiche.fco_fch_oa,  etat_initial.fco_efi_date AS fco_efi_date_creation,  etat_courant.fco_efi_date AS fco_efi_date,  grp_initial.grp_libelle AS grp_initiateur_libelle,  initiateur.uti_prenom AS uti_initiateur_prenom,  initiateur.uti_nom AS uti_initiateur_nom,  etat_courant.fco_efi_code_etat AS fco_efi_code_etat,  categorie.fco_cat_libelle AS fco_cat_libelle,  categorie.fco_sous_cat_libelle AS fco_sous_cat_libelle,  priorite.fco_pri_libelle AS fco_pri_libelle
FROM
	fco_fiche fiche INNER JOIN fco_etat_fiche etat_courant ON etat_courant.fco_efi_id = fiche.fco_efi_id_courant  INNER JOIN fco_priorite priorite ON priorite.fco_pri_code = fiche.fco_pri_code  INNER JOIN fco_categorie categorie ON categorie.fco_cat_code = fiche.fco_cat_code AND categorie.fco_sous_cat_code = fiche.fco_sous_cat_code  INNER JOIN fco_etat_fiche etat_initial ON etat_initial.fco_efi_id = fiche.fco_efi_id_initial  INNER JOIN groupe grp_initial ON grp_initial.grp_id = etat_initial.grp_id_auteur  INNER JOIN utilisateur initiateur ON initiateur.uti_id = etat_initial.uti_id_auteur
WHERE
	etat_courant.uti_id_dest = $1  OR (   etat_courant.uti_id_dest IS NULL  AND etat_courant.grp_id_dest IN ($2, $3) ) ORDER BY fiche.fco_fch_id ASC OFFSET $4 LIMIT $5 "};

my $fixture_filtered_query = q{"SELECT 	   fiche.fco_fch_id, fiche.fco_efi_id_initial, fiche.fco_efi_id_courant, fiche.fco_cat_code, fiche.fco_sous_cat_code, fiche.fco_pri_code, fiche.fco_fch_titre, fiche.fco_fch_da, fiche.fco_fch_ej, fiche.fco_fch_dp, fiche.fco_fch_tiers, fiche.fco_fch_sf, fiche.fco_fch_centre_financier, fiche.fco_fch_numero_facture, fiche.fco_fch_oa,  etat_initial.fco_efi_date AS fco_efi_date_creation,  etat_courant.fco_efi_date AS fco_efi_date,  grp_initial.grp_libelle AS grp_initiateur_libelle,  initiateur.uti_prenom AS uti_initiateur_prenom,  initiateur.uti_nom AS uti_initiateur_nom,  etat_courant.fco_efi_code_etat AS fco_efi_code_etat,  categorie.fco_cat_libelle AS fco_cat_libelle,  categorie.fco_sous_cat_libelle AS fco_sous_cat_libelle,  priorite.fco_pri_libelle AS fco_pri_libelle FROM 	fco_fiche fiche INNER JOIN fco_etat_fiche etat_courant ON etat_courant.fco_efi_id = fiche.fco_efi_id_courant  INNER JOIN fco_priorite priorite ON priorite.fco_pri_code = fiche.fco_pri_code  INNER JOIN fco_categorie categorie ON categorie.fco_cat_code = fiche.fco_cat_code AND categorie.fco_sous_cat_code = fiche.fco_sous_cat_code  INNER JOIN fco_etat_fiche etat_initial ON etat_initial.fco_efi_id = fiche.fco_efi_id_initial  INNER JOIN groupe grp_initial ON grp_initial.grp_id = etat_initial.grp_id_auteur  INNER JOIN utilisateur initiateur ON initiateur.uti_id = etat_initial.uti_id_auteur WHERE 	etat_courant.uti_id_dest = $1  OR (   etat_courant.uti_id_dest IS NULL  AND etat_courant.grp_id_dest IN ($2, $3) ) ORDER BY fiche.fco_fch_id ASC OFFSET $4 LIMIT $5 "};

my $fixture_log_entry = q{2012-11-12 10:48:51.126 CET,"test_app","test_app",30572,"172.19.124.16:5667",50a0ba59.776c,40,"SELECT",2012-11-12 09:59:05 CET,95/4451,0,ERROR,57014,"canceling statement due to statement timeout",,,,,,} . $fixture_query . q{,,"ProcessInterrupts, postgres.c:2672"
};

my $fixture_log_output = q{"2012-11-12 10:48:51.126 CET",test_app,test_app,30572,172.19.124.16:5667,50a0ba59.776c,40,SELECT,"2012-11-12 09:59:05 CET",95/4451,0,ERROR,57014,"canceling statement due to statement timeout",,,,,,} . $fixture_filtered_query . q{,,"ProcessInterrupts, postgres.c:2672"
};


### Tests

sub test_output {
	my $result = q{};
	my $output = output \$result;
	my $input = input \$fixture_log_entry;
	my $line = $input->();
	$output->($line);
	eval { assert_equals $result, $fixture_log_output } ; assert_passed;
}

sub test_input {
	my $input = input \$fixture_log_entry;
	my $line = $input->();

	eval { assert_equals $line->[LOG_TIME], '2012-11-12 10:48:51.126 CET' } ; assert_passed;
	eval { assert_equals $line->[USER_NAME], 'test_app' } ; assert_passed;
	eval { assert_equals $line->[DATABASE_NAME], 'test_app' } ; assert_passed;
	eval { assert_equals $line->[PROCESS_ID], 30572 } ; assert_passed;
	eval { assert_equals $line->[CONNECTION_FROM], '172.19.124.16:5667' } ; assert_passed;
	eval { assert_equals $line->[SESSION_ID], '50a0ba59.776c' } ; assert_passed;
	eval { assert_equals $line->[SESSION_LINE_NUM], 40 } ; assert_passed;
	eval { assert_equals $line->[COMMAND_TAG], 'SELECT' } ; assert_passed;
	eval { assert_equals $line->[SESSION_START_TIME], '2012-11-12 09:59:05 CET' } ; assert_passed;
	eval { assert_equals $line->[VIRTUAL_TRANSACTION_ID], '95/4451' } ; assert_passed;
	eval { assert_equals $line->[TRANSACTION_ID], 0 } ; assert_passed;
	eval { assert_equals $line->[ERROR_SEVERITY], 'ERROR' } ; assert_passed;
	eval { assert_equals $line->[SQL_STATE_CODE], '57014' } ; assert_passed;
	eval { assert_equals $line->[MESSAGE], 'canceling statement due to statement timeout' } ; assert_passed;
	eval { assert_equals $line->[LOCATION], 'ProcessInterrupts, postgres.c:2672' } ; assert_passed;
	eval { assert_none $line->[DETAIL] } ; assert_passed;
	eval { assert_none $line->[HINT] } ; assert_passed;
	eval { assert_none $line->[INTERNAL_QUERY] } ; assert_passed;
	eval { assert_none $line->[INTERNAL_QUERY_POS] } ; assert_passed;
	eval { assert_none $line->[CONTEXT] } ; assert_passed;
	eval { assert_none $line->[QUERY_POS] } ; assert_passed;
	eval { assert_equals q{"} . $line->[QUERY] . q{"}, $fixture_filtered_query; } ; assert_passed;
}

sub test_collect_all {
	my $result = q{};
	my $I = sub {return shift;};
	collect_all $I, \$fixture_log_entry, \$result;
	eval { assert_equals $result, $fixture_log_output } ; assert_passed;
}

sub test_reduce_all {
	my $count_timeouts = count_if sub {
		my $entry = shift;
		return $entry->[MESSAGE] && $entry->[MESSAGE] =~ /imeout/;
	};
	my $result = q{};
	my $output = output \$result;
	my @reducers = ($count_timeouts);
	my @current = (0);
	$output->(tail multi_reduce \@reducers, \@current, input \$fixture_log_entry);
	eval { assert_equals "1\n", $result; } ; assert_passed;
}

run_tests 'pg_csv_log_parser' if $0 =~ /pg_csv_log_parser.pm$/;

1;
