#!/usr/bin/env perl

use strict;
use warnings;

use Test::More;
use Test::Deep;

use AnyEvent::KyotoTycoon ();


my %req = (ct => 'text/tab-separated-values', body => '');
my %res = (code => 200, %req);
my $kt = AnyEvent::KyotoTycoon->new;


{
	no warnings 'redefine';
	*AnyEvent::KyotoTycoon::http_post = sub ($$@) {
		my $cb = pop();
		my ($url, $body, %args) = @_;

		is $url,  'http://127.0.0.1:1978/rpc/void';
		is $body, $req{body};
		cmp_deeply \%args, superhashof {timeout => 1};
		is $args{headers}{'content-type'}, $req{ct};

		$cb->($res{body}, {
			'Status'       => $res{code},
			'content-type' => $res{ct},
		});
	};
}

# $kt->void($cb->($ret));
$kt->void(sub {
	ok $_[0];
});

{
	no warnings 'redefine';
	*AnyEvent::KyotoTycoon::http_post = sub ($$@) {
		my $cb = pop();
		my (undef, $body) = @_;

		if (ref($req{body})) {
			like $body, $req{body};
		}
		else {
			is $body, $req{body};
		}

		$cb->($res{body}, {
			'Status'       => $res{code},
			'Reason'       => '',
			'content-type' => $res{ct},
		});
	};
}

# $kt->echo([\%args, ]$cb->(\%vals));
$res{body} = $req{body} = "foo\tbar";
$kt->echo({foo => 'bar'}, sub {
	cmp_deeply $_[0], {foo => 'bar'};
});
$res{body} = $req{body} = '';
$kt->echo(sub {
	cmp_deeply $_[0], {};
});

# $kt->report($cb->(\%vals));
$res{body} = "foo\tbar";
$kt->report(sub {
	cmp_deeply $_[0], {foo => 'bar'};
});

# $kt->play_script($name, [\%args, ]$cb->(\%vals));
$req{body} = "name\tbaz";
$res{body} = "_foo\tbar";
$kt->play_script('baz', sub {
	cmp_deeply $_[0], {foo => 'bar'};
});
$res{code} = 450;
$res{body} = "ERROR\terror";
$kt->play_script('baz', sub {
	ok !defined($_[0]);
});
$res{code} = 200;
$req{body} = qr/name\tbaz\n_bar\tfoo|_bar\tfoo\nname\tbaz/;
$res{body} = "_foo\tbar";
$kt->play_script('baz', {bar => 'foo'}, sub {
	cmp_deeply $_[0], {foo => 'bar'};
});

# $kt->status([$db, ]$cb->(\%vals));
$req{body} = '';
$res{body} = "count\t1\nsize\t2\nfoo\tbar";
$kt->status(sub {
	cmp_deeply $_[0], {foo => 'bar', count => ignore, size => ignore};
});
$req{body} = "DB\tdb";
$kt->status('db', sub {
	cmp_deeply $_[0], {foo => 'bar', count => ignore, size => ignore};
});

# $kt->clear([$db, ]$cb->($ret));
$req{body} = '';
$res{body} = '';
$kt->clear(sub {
	ok $_[0];
});
$req{body} = "DB\tdb";
$kt->status('db', sub {
	ok $_[0];
});

# $kt->set($key, $val, [$xt, [$db, ]]$cb->($ret));
$req{body} = qr/key\tfoo\nvalue\tbar\nxt\t1\nDB\tdb|
                key\tfoo\nvalue\tbar\nDB\tdb\nxt\t1|
                key\tfoo\nDB\tdb\nxt\t1\nvalue\tbar|
                key\tfoo\nxt\t1\nDB\tdb\nvalue\tbar|
                value\tbar\nxt\t1\nDB\tdb\nkey\tfoo|
                value\tbar\nDB\tdb\nxt\t1\nkey\tfoo|
                DB\tdb\nxt\t1\nvalue\tbar\nkey\tfoo|
                xt\t1\nDB\tdb\nvalue\tbar\nkey\tfoo/x;
$res{body} = '';
$kt->set(foo => 'bar', 1, 'db', sub {
	ok $_[0];
});

# $kt->increment($key, $val, [$xt, [$db, ]]$cb->($val));
# $kt->increment_double($key, $val, [$xt, [$db, ]]$cb->($val));
$req{body} = qr/key\tfoo\n1\tbar|num\t1\nkey\tfoo/;
$res{body} = "num\t2";
$kt->increment(foo => 1, sub {
	is $_[0], 2;
});
$res{code} = 450;
$res{body} = "ERROR\terror";
$req{body} = qr/key\tfoo\nnum\tbar|num\tbar\nkey\tfoo/;
$kt->increment(foo => 'bar', sub {
	ok !defined($_[0]);
});

# $kt->get($key, [$db, ]$cb->([$val, $xt]));
$res{code} = 200;
$req{body} = "key\tfoo";
$res{body} = "value\tbar";
$kt->get('foo', sub {
	is scalar(@_), 2;
	is $_[0], 'bar';
	ok !defined($_[1]);
});
$res{code} = 450;
$res{body} = "ERROR\terror";
$kt->get('foo', sub {
	ok !defined($_[0]);
});


done_testing;
