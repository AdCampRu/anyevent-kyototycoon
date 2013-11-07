#!/usr/bin/env perl

use strict;
use warnings;

use Test::More;


plan(skip_all => 'Set RELEASE_TESTING to enable this test (developer only)')
	unless $ENV{RELEASE_TESTING};
plan(skip_all => 'Test::Fixme required for this test')
	unless eval('use Test::Fixme; 1');

run_tests(where => 'lib', match => qr/not implemented/i);
done_testing();
