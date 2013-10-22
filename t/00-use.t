#!/usr/bin/env perl

use strict;
use warnings;

use Test::More tests => 1;


BEGIN {
	use_ok('AnyEvent::KyotoTycoon') or print("Bail out!\n");
}

diag("Testing AnyEvent::KyotoTycoon $AnyEvent::KyotoTycoon::VERSION, Perl $], $^X");
