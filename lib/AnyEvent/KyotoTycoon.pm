package AnyEvent::KyotoTycoon;

use strict;
use warnings;

use AnyEvent ();
use AnyEvent::HTTP qw(http_post);

our $VERSION = '0.01';


sub new {
	my ($proto, %args) = @_;

	return bless(
		{
			server  => $args{server}  // '127.0.0.1:1978',
			db      => $args{db},
			timeout => $args{timeout} // 1,
		},
		ref($proto) || $proto
	);
}

sub db {
	if (@_ > 1) {
		$_[0]->{db} = $_[1];
	}

	return $_[0]->{db};
}

# $kt->void($cb->($ret));
sub void {
	my $cb = pop();
	my ($self) = @_;

	$self->call(
		'void',
		{},
		sub {
			$cb->($_[0] ? (1) : ());
		}
	);
}

# $kt->echo([\%args, ]$cb->(\%vals));
sub echo {
	my $cb = pop();
	my ($self, $args) = @_;

	$self->call('echo', $args, $cb);
}

# $kt->report($cb->(\%vals));
sub report {
	my $cb = pop();
	my ($self) = @_;

	$self->call('report', {}, $cb);
}

# $kt->play_script($name, [\%args, ]$cb->(\%vals));
sub play_script {
	my $cb = pop();
	my ($self, $name, $args) = @_;

	$self->call(
		'play_script',
		{name => $name, ref($args) eq 'HASH' ? map { ('_' . $_ => $args->{$_}) } keys(%$args) : ()},
		sub {
			$cb->($_[0] ? {map { (substr($_, 1) => $_[0]{$_}) } keys(%{$_[0]})} : ());
		}
	);
}

sub tune_replication {
	my $cb = pop();

	AE::log(error => 'Procedure "tune_replication" not implemented');
	$cb->(1);
}

# $kt->status([$db, ]$cb->(\%vals));
sub status {
	my $cb = pop();
	my ($self, $db) = @_;

	$db //= $self->{db};

	$self->call(
		'status',
		{defined($db) ? (DB => $db) : ()},
		$cb
	);
}

# $kt->clear([$db, ]$cb->($ret));
sub clear {
	my $cb = pop();
	my ($self, $db) = @_;

	$db //= $self->{db};

	$self->call(
		'clear',
		{defined($db) ? (DB => $db) : ()},
		sub {
			$cb->($_[0] ? (1) : ());
		}
	);
}

# $kt->set($key, $val, [$xt, [$db, ]]$cb->($ret));
sub set {
	my $cb = pop();
	my ($self, $key, $val, $xt, $db) = @_;

	$db //= $self->{db};

	$self->call(
		'set',
		{key => $key, value => $val, (defined($xt) ? (xt => $xt) : ()), (defined($db) ? (DB => $db) : ())},
		sub {
			$cb->($_[0] ? (1) : ());
		}
	);
}

# $kt->get($key, [$db, ]$cb->([$val, $xt]));
sub get {
	my $cb = pop();
	my ($self, $key, $db) = @_;

	$db //= $self->{db};

	$self->call(
		'get',
		{key => $key, (defined($db) ? (DB => $db) : ())},
		sub {
			$cb->($_[0] ? ($_[0]->{value}, $_[0]->{xt}) : ());
		}
	);
}

sub call {
	my $cb = pop();
	my ($self, $meth, $data, $enc) = @_;

	$enc //= '';

	my $body  = AnyEvent::KyotoTycoon::_Util::encode_tsv($data, $enc);

	$self->_request(
		'http://' . $self->{server} . '/rpc/' . $meth,
		$body,
		$enc,
		sub {
			my ($body, $enc) = @_;

			unless (defined($enc)) {
				$cb->();
				return;
			}

			my $data = AnyEvent::KyotoTycoon::_Util::decode_tsv($body, $enc);

			unless ($data) {
				AE::log(error => 'Body decoding failed');
				$cb->();
				return;
			}
			if (exists($data->{ERROR})) {
				AE::log(error => 'Server error raised: ' . $data->{ERROR});
				$cb->();
				return;
			}

			$cb->($data);
			return;
		}
	);
}

sub _request {
	my ($self, $url, $body, $enc, $cb) = @_;

	http_post(
		$url,
		$body,
		headers    => {'content-type' => 'text/tab-separated-values' . ($enc ? '; colenc=' . $enc : '')},
		timeout    => $self->{timeout},
		persistent => 1,
		keepalive  => 1,
		recurse    => 0,
		sub {
			my ($body, $hdrs) = @_;

			my $enc = AnyEvent::KyotoTycoon::_Util::check_encoding($hdrs->{'content-type'});

			unless ($hdrs->{Status} == 200) {
				AE::log(error => 'Request failed: ' . $hdrs->{Status} . ' (' . $hdrs->{Reason} . ')');
				if ($hdrs->{Status} >= 590) {
					$cb->();
					return;
				}
			}
			unless (defined($enc)) {
				AE::log(error => 'Unknown content type received: ' . ($hdrs->{'content-type'} // 'undef'));
			}

			$cb->($body, $enc);
			return;
		}
	);
}


BEGIN {
	package AnyEvent::KyotoTycoon::_Util;

	use strict;
	use warnings;

	require MIME::QuotedPrint;
	require MIME::Base64;

	my %ENCODERS = (
		''  => sub { $_[0] },
		'Q' => sub { MIME::QuotedPrint::encode_qp($_[0], '') },
		'B' => sub { MIME::Base64::encode_base64($_[0], '') },
	);
	my %DECODERS = (
		''  => $ENCODERS{''},
		'Q' => sub { MIME::QuotedPrint::decode_qp($_[0]) },
		'B' => sub { MIME::Base64::decode_base64($_[0]) },
	);

	eval {
		require URI::Escape::XS;
		$ENCODERS{U} = \&URI::Escape::XS::uri_escape;
		$DECODERS{U} = \&URI::Escape::XS::uri_unescape;
	};
	if ($@) {
		require URI::Escape;
		$ENCODERS{U} = \&URI::Escape::uri_escape;
		$DECODERS{U} = \&URI::Escape::uri_unescape;
	}

	sub encode_tsv {
		my ($data, $enc) = @_;

		return unless ref($data) eq 'HASH';

		my $sub = $enc ? $ENCODERS{$enc} // $ENCODERS{''} : $ENCODERS{''};

		return join(
			"\n",
			map {
				$sub->($_) . "\t" . $sub->($data->{$_} // '');
			} keys(%$data)
		);
	}

	sub decode_tsv {
		my ($data, $enc) = @_;

		return if !defined($data) || ref($data);

		my $sub = $enc ? $DECODERS{$enc} // $DECODERS{''} : $DECODERS{''};

		return {
			map {
				map { $sub->($_) } (split(/\t/, $_, 2), '')[0, 1];
			}
			grep { length }
			split(/\n/, $data)
		};
	}

	sub check_encoding {
		return unless defined($_[0]);
		return $1 // '' if $_[0] =~ /text\/tab\-separated\-values(?:; colenc=([BUQ]))?/;
		return;
	}
}


1;
