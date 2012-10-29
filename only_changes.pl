#!/usr/bin/perl
use warnings;
use strict;
my $last_count = '0';
my $last_line = '';
my $last_print = '';
while(<STDIN>) {
    unless(m/$last_count$/) {
	if(m/(,\d+)$/) {
	    print $last_line if $last_line ne $last_print;
	    print;
	    $last_print = $_;
	    $last_count = $1;
	}
    }
    $last_line = $_;
}

