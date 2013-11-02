#!/usr/bin/env perl

use strict;
use warnings;

use ExtUtils::MakeMaker;


WriteMakefile(
	NAME             => 'AnyEvent::KyotoTycoon',
	AUTHOR           => 'Denis Ibaev <dionys@cpan.org>',
	VERSION_FROM     => 'lib/AnyEvent/KyotoTycoon.pm',
	ABSTRACT_FROM    => 'lib/AnyEvent/KyotoTycoon.pm',
	LICENSE          => 'perl',
	MIN_PERL_VERSION => '5.10',
	BUILD_REQUIRES   => {
		'Test::Deep'        => 0,
		'Test::More'        => 0,
	},
	PREREQ_PM        => {
		'AnyEvent'          => 0,
		'AnyEvent::HTTP'    => 0,
		'MIME::Base64'      => 0,
		'MIME::QuotedPrint' => 0,
		'URI::Escape'       => 0,
	},
	META_MERGE       => {
		recommends => {
			'URI::Escape::XS' => 0,
		},
		resources  => {
			license     => 'http://dev.perl.org/licenses/',
			repository  => 'http://github.com/AdCampRu/anyevent-kyototycoon',
			bugtracker  => 'http://github.com/AdCampRu/anyevent-kyototycoon/issues',
		},
		no_index   => {directory => ['t']},
	},
	dist             => {COMPRESS => 'gzip -9f', SUFFIX => 'gz'},
	clean            => {FILES => 'AnyEvent-KyotoTycoon-*'},
	test             => {TESTS => 't/*.t'},
);
