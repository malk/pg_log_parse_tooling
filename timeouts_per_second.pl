#!/usr/bin/perl
use warnings;
use strict;
my $timestamp = '';
my $timeouts = 0;
while(<STDIN>) {
  if(m/(\d{2}:\d{2}:\d{2})/ && $timestamp ne $1) {
    print $timestamp, ',', $timeouts, "\n" unless $timestamp eq '';
    $timestamp = $1;
    $timeouts = 0;
  }
  $timeouts++ if m/timeout/;
}
print $timestamp, ',', $timeouts, "\n";
