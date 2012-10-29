#!/usr/bin/perl
use warnings;
use strict;
my $during_timeout = 0;
while(<STDIN>) {
    $during_timeout = 1 if m/timeout/;
    if($during_timeout) {
	if(m/STATEMENT/ || m/LOCATION/ || m/LOG/) {
	    print;
	} elsif(m/DETAIL/) {
	    print;
	    $during_timeout = 0;
	}
    }
}
