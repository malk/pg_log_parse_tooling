#!/usr/bin/perl
use warnings;
use strict;
my $start = $ARGV[0] || 0;
my $end = $ARGV[1] || 0;


if($start) {
  my $l;
  do {
    $l = <STDIN>;
  } while($l !~ m/$start/);
  print $l;
}

if($end) {
  my $l;
  do {
    $l = <STDIN>;
    print $l;
  } while($l !~ m/$end/);
} else {
  while(<STDIN>) {
    print;
  }
}

