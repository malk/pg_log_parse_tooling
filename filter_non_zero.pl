#!/usr/bin/perl
use warnings;
use strict;
while(<STDIN>) {
  print unless m/,0$/;
}

