#!/usr/bin/env perl

use strict;
use warnings;

use Test::More;
use Test::Deep;

use AnyEvent::KyotoTycoon::Util qw(fetch_ret_cb fetch_val_cb fetch_vals_cb);


*ret_cb  = *fetch_ret_cb;
*val_cb  = *fetch_val_cb;
*vals_cb = *fetch_vals_cb;


ret_cb(sub {
	is scalar(@_), 0;
})->();
ret_cb(sub {
	is scalar(@_), 1;
	is $_[0], 1;
})->({});
ret_cb(sub {
	is scalar(@_), 1;
	is $_[0], 1;
})->({foo => 'bar'});

val_cb(sub {
	is scalar(@_), 0;
})->();
val_cb(sub {
	is scalar(@_), 0;
})->({});
val_cb(sub {
	is scalar(@_), 0;
})->({foo => 'bar'});
val_cb('foo', sub {
	is scalar(@_), 0;
})->();
val_cb('foo', sub {
	is scalar(@_), 1;
	is $_[0], undef;
})->({});
val_cb('foo', sub {
	is scalar(@_), 1;
	is $_[0], 'bar';
})->({foo => 'bar'});
val_cb('foo', sub {
	is scalar(@_), 1;
	is $_[0], 'bar';
})->({foo => 'bar', baz => 'qux'});
val_cb('foo', 'baz', sub {
	is scalar(@_), 2;
	is $_[0], 'bar';
	is $_[1], undef;
})->({foo => 'bar'});
val_cb('foo', 'baz', sub {
	is scalar(@_), 2;
	is $_[0], undef;
	is $_[1], 'qux';
})->({baz => 'qux'});
val_cb('foo', 'baz', sub {
	is scalar(@_), 2;
	is $_[0], 'bar';
	is $_[1], 'qux';
})->({foo => 'bar', baz => 'qux'});

vals_cb(sub {
	is scalar(@_), 0;
})->();
vals_cb(sub {
	is scalar(@_), 1;
	cmp_deeply $_[0], {};
})->({});
vals_cb(sub {
	is scalar(@_), 1;
	cmp_deeply $_[0], {foo => 'bar'};
})->({_foo => 'bar'});
vals_cb(sub {
	is scalar(@_), 1;
	cmp_deeply $_[0], {foo => 'bar'};
})->({_foo => 'bar', num => 1});


done_testing();
