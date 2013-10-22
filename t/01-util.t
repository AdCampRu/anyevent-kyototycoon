#!/usr/bin/env perl

use strict;
use warnings;

use Test::More;
use Test::Deep;

use AnyEvent::KyotoTycoon ();


*encode = *AnyEvent::KyotoTycoon::_Util::encode_tsv;
*decode = *AnyEvent::KyotoTycoon::_Util::decode_tsv;
*check  = *AnyEvent::KyotoTycoon::_Util::check_encoding;


is encode(),   undef;
is encode([]), undef;
is encode({}), '';

like encode({foo => 'bar', baz => 'qux'}),      qr/foo\tbar\nbaz\tqux|baz\tqux\nfoo\tbar/;
like encode({foo => 'bar', baz => 'qux'}, ''),  qr/foo\tbar\nbaz\tqux|baz\tqux\nfoo\tbar/;
like encode({foo => 'bar', baz => 'qux'}, 'T'), qr/foo\tbar\nbaz\tqux|baz\tqux\nfoo\tbar/;
like encode({foo => 'bar', baz => 'qux'}, 'U'), qr/foo\tbar\nbaz\tqux|baz\tqux\nfoo\tbar/;
like encode({foo => "\x03b2\x03b1\x03c1", "\x03b2\x03b1\x03b6" => 'qux'}, 'U'),
                                                qr/foo\t%03b2%03b1%03c1\n%03b2%03b1%03b6\tqux|%03b2%03b1%03b6\tqux\nfoo\t%03b2%03b1%03c1/;
like encode({foo => 'bar', baz => 'qux'}, 'B'), qr/Zm9v\tYmFy\nYmF6\tcXV4|YmF6\tcXV4\nZm9v\tYmFy/;
like encode({foo => 'bar', baz => 'qux'}, 'Q'), qr/foo\tbar\nbaz\tqux|baz\tqux\nfoo\tbar/;
like encode({foo => "\x03b2\x03b1\x03c1", "\x03b2\x03b1\x03b6" => 'qux'}, 'Q'),
                                                qr/foo\t=03b2=03b1=03c1\n=03b2=03b1=03b6\tqux|=03b2=03b1=03b6\tqux\nfoo\t=03b2=03b1=03c1/;

is decode(),   undef;
is decode([]), undef;
cmp_deeply decode(''),           {};
cmp_deeply decode('foo'),        {foo => ''};
cmp_deeply decode("foo\n\nbar"), {foo => '', bar => ''};

cmp_deeply decode("foo\tbar\nbaz\tqux"),      {foo => 'bar', baz => 'qux'};
cmp_deeply decode("foo\tbar\nbaz\tqux", ''),  {foo => 'bar', baz => 'qux'};
cmp_deeply decode("foo\tbar\nbaz\tqux", 'T'), {foo => 'bar', baz => 'qux'};
cmp_deeply decode("foo\tbar\nbaz\tqux", 'U'), {foo => 'bar', baz => 'qux'};
cmp_deeply decode("foo\t%03b2%03b1%03c1\n%03b2%03b1%03b6\tqux", 'U'),
                                              {foo => "\x03b2\x03b1\x03c1", "\x03b2\x03b1\x03b6" => 'qux'};
cmp_deeply decode("Zm9v\tYmFy\nYmF6\tcXV4", 'B'), {foo => 'bar', baz => 'qux'};
cmp_deeply decode("foo\tbar\nbaz\tqux", 'Q'), {foo => 'bar', baz => 'qux'};
cmp_deeply decode("foo\t=03b2=03b1=03c1\n=03b2=03b1=03b6\tqux", 'Q'),
                                              {foo => "\x03b2\x03b1\x03c1", "\x03b2\x03b1\x03b6" => 'qux'};

is check(),      undef;
is check(''),    undef;
is check('foo'), undef;

is check('text/tab-separated-values'),           '';
is check('text/tab-separated-values; colenc=T'), '';
is check('text/tab-separated-values; colenc=U'), 'U';
is check('text/tab-separated-values; colenc=B'), 'B';
is check('text/tab-separated-values; colenc=Q'), 'Q';

done_testing;
