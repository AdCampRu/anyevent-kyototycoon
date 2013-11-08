package
	AnyEvent::KyotoTycoon::TSVRPC;

use strict;
use warnings;

use parent 'Exporter';


our @EXPORT    = qw(encode_tsv decode_tsv check_encoding);
our @EXPORT_OK = @EXPORT;


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


1;
