#!/usr/bin/perl
package pg_csv_log_parser_tests;

use warnings;
use strict;
use Test::Extreme;
use pg_csv_log_parser;

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

sub test_fix {
	my @entries = (1, 2, 1);
	my @expected = ();
	my $shifter = sub { return shift @entries; };
	fix $shifter;
	eval { assert_equals @expected, @entries } ; assert_passed;
}


sub test_iterate {
	my @entries = (1, 2, 1);
	my @expected = ();
	my $shifter = sub { return shift @entries; };
	iterate $shifter;
	eval { assert_equals @expected, @entries } ; assert_passed;
}


sub test_tail {
	eval { assert_equals 1, tail shifter(3, 2, 1) } ; assert_passed;
}


sub test_assert_equals_sequences {
	eval { assert_equals_sequences shifter(1, 2, 1), shifter(1, 2, 1); } ; assert_passed;
	eval { assert_equals_sequences shifter(1, 2, 1), shifter(2, 1, 2); } ; assert_failed;
}


sub test_collect {
	my $increment = sub { return 1 + shift; };
	eval { assert_equals_sequences shifter(2, 3, 2), collect $increment, shifter(1, 2, 1); } ; assert_passed;
	eval { assert_equals_sequences shifter(2, 3, 2), collect $increment, shifter(2, 1, 2); } ; assert_failed;
}


sub test_filter {
	my $predicate = sub {return 0;};
	eval { assert_equals_sequences shifter(), filter $predicate, shifter(1, 2, 3, 4, 5, 6); } ; assert_passed;
	$predicate = sub {return shift(@_) % 2;};
	eval { assert_equals_sequences shifter(1, 3, 5), filter $predicate, shifter(1, 2, 3, 4, 5, 6); } ; assert_passed;
}


sub test_head_cut {
	my $predicate = sub {return 4 == shift;};
	eval { assert_equals_sequences shifter(4, 5, 6), head_cut $predicate, shifter(1, 2, 3, 4, 5, 6); } ; assert_passed;
}

sub test_stopgag {
	my $predicate = sub {return 4 == shift;};
	eval { assert_equals_sequences shifter(1, 2, 3), stopgag $predicate, shifter(1, 2, 3, 4, 5, 6); } ; assert_passed;
}


sub test_reduce {
	my $adder = sub {return shift(@_) + shift(@_)};
	eval { assert_equals 6, fix reduce $adder, 0, shifter(1, 2, 3); } ; assert_passed;
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

run_tests 'pg_csv_log_parser_tests';

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
