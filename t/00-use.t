#!/usr/bin/env perl

use strict;
use warnings;

use Test::More tests => 2;


BEGIN {
	use_ok('AnyEvent::KyotoTycoon') or print("Bail out!\n");
	use_ok('AnyEvent::KyotoTycoon::Cursor') or print("Bail out!\n");
}

diag("Testing AnyEvent::KyotoTycoon $AnyEvent::KyotoTycoon::VERSION, Perl $], $^X");
