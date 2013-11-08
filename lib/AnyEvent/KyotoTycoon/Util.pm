package
	AnyEvent::KyotoTycoon::Util;

use strict;
use warnings;

use parent 'Exporter';


our @EXPORT    = qw(fetch_ret_cb fetch_val_cb fetch_vals_cb);
our @EXPORT_OK = @EXPORT;


sub fetch_ret_cb {
	my ($cb) = @_;

	return sub {
		$cb->($_[0] ? 1 : ());
	}	
}

sub fetch_val_cb {
	my $cb = pop();
	my @keys = @_;

	return sub {
		if ($_[0]) {
			$cb->(map { $_[0]{$_} } @keys);
		}
		else {
			$cb->();
		}
	};	
}

sub fetch_vals_cb {
	my ($cb) = @_;

	return sub {
		if ($_[0]) {
			delete($_[0]{num});
			$cb->({map { (substr($_, 1) => $_[0]{$_}) } keys(%{$_[0]})});
		}
		else {
			$cb->();
		}
	};
}


1;
