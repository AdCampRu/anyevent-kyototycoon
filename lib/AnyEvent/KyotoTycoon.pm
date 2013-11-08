package AnyEvent::KyotoTycoon;

use strict;
use warnings;

use AnyEvent ();
use AnyEvent::HTTP qw(http_post);

use AnyEvent::KyotoTycoon::TSVRPC qw(encode_tsv decode_tsv check_encoding);
use AnyEvent::KyotoTycoon::Util qw(fetch_ret_cb fetch_val_cb fetch_vals_cb);


our $VERSION = '0.01';


sub new {
	my ($proto, %args) = @_;

	return bless(
		{
			server   => $args{server}   // '127.0.0.1:1978',
			database => $args{database},
			encoding => $args{encoding} // '',
			timeout  => $args{timeout}  // 1,
		},
		ref($proto) || $proto
	);
}

sub database {
	$_[0]->{database} = $_[1] if @_ > 1;
	return $_[0]->{database};
}

sub encoding {
	$_[0]->{encoding} = $_[1] if @_ > 1;
	return $_[0]->{encoding};
}

# $kt->void($cb->($ret));
sub void {
	my $cb = pop();
	my ($self) = @_;

	$self->call('void', {}, fetch_ret_cb($cb));
}

# $kt->echo($cb->(\%vals));
# $kt->echo(\%args, $cb->(\%vals));
# $kt->echo(\%args, %opts, $cb->(\%vals));
sub echo {
	my $cb = pop();
	my ($self, $args, %opts) = @_;

	$self->call('echo', $args // {}, %opts, $cb);
}

# $kt->report($cb->(\%vals));
# $kt->report(%opts, $cb->(\%vals));
sub report {
	my $cb = pop();
	my ($self, %opts) = @_;

	$self->call('report', {}, %opts, $cb);
}

# $kt->play_script($name, $cb->(\%vals));
# $kt->play_script($name, \%args, $cb->(\%vals));
# $kt->play_script($name, \%args, %opts, $cb->(\%vals));
sub play_script {
	my $cb = pop();
	my ($self, $name, $args, %opts) = @_;

	$self->call(
		'play_script',
		{
			name => $name,
			ref($args) eq 'HASH' ? map { ('_' . $_ => $args->{$_}) } keys(%$args) : (),
		},
		%opts,
		fetch_vals_cb($cb)
	);
}

sub tune_replication {
	my $cb = pop();

	AE::log(error => 'Procedure "tune_replication" not implemented');
	$cb->();
}

# $kt->status($cb->(\%vals));
# $kt->status(%opts, $cb->(\%vals));
sub status {
	my $cb = pop();
	my ($self, %opts) = @_;

	my $db = exists($opts{database}) ? $opts{database} : $self->{database};

	$self->call('status', {defined($db) ? (DB => $db) : ()}, $cb);
}

# $kt->clear($cb->($ret));
# $kt->clear(%opts, $cb->($ret));
sub clear {
	my $cb = pop();
	my ($self, %opts) = @_;

	my $db = exists($opts{database}) ? $opts{database} : $self->{database};

	$self->call('clear', {defined($db) ? (DB => $db) : ()}, fetch_ret_cb($cb));
}

sub _set {
	my $cb = pop();
	my ($self, $proc, $key, $val, $xt, %opts) = @_;

	my $db = exists($opts{database}) ? delete($opts{database}) : $self->{database};

	$self->call(
		$proc,
		{
			key   => $key,
			value => $val,
			(defined($xt) ? (xt => $xt) : ()),
			(defined($db) ? (DB => $db) : ()),
		},
		%opts,
		fetch_ret_cb($cb)
	);
}

# $kt->set($key, $val, $cb->($ret));
# $kt->set($key, $val, $xt, $cb->($ret));
# $kt->set($key, $val, $xt, %opts, $cb->($ret));
sub set { shift()->_set('set', @_); }

# $kt->add($key, $val, $cb->($ret));
# $kt->add($key, $val, $xt, $cb->($ret));
# $kt->add($key, $val, $xt, %opts, $cb->($ret));
sub add { shift()->_set('add', @_); }

# $kt->replace($key, $val, $cb->($ret));
# $kt->replace($key, $val, $xt, $cb->($ret));
# $kt->replace($key, $val, $xt, %opts, $cb->($ret));
sub replace { shift()->_set('replace', @_); }

# $kt->append($key, $val, $cb->($ret));
# $kt->append($key, $val, $xt, $cb->($ret));
# $kt->append($key, $val, $xt, %opts, $cb->($ret));
sub append { shift()->_set('append', @_); }

sub _increment {
	my $cb = pop();
	my ($self, $proc, $key, $val, $xt, %opts) = @_;

	my $db = exists($opts{database}) ? delete($opts{database}) : $self->{database};

	$self->call(
		$proc,
		{key => $key, num => $val, (defined($xt) ? (xt => $xt) : ()), (defined($db) ? (DB => $db) : ())},
		%opts,
		fetch_val_cb('num', $cb)
	);
}

# $kt->increment($key, $val, $cb->($val));
# $kt->increment($key, $val, $xt, $cb->($val));
# $kt->increment($key, $val, $xt, %opts, $cb->($val));
sub increment { shift()->_increment('increment', @_); }

# $kt->increment_double($key, $val, $cb->($val));
# $kt->increment_double($key, $val, $xt, $cb->($val));
# $kt->increment_double($key, $val, $xt, %opts, $cb->($val));
sub increment_double { shift()->_increment('increment_double', @_); }

# $kt->cas($key, $cmp, $val, $cb->($ret));
# $kt->cas($key, $cmp, $val, $xt, $cb->($ret));
# $kt->cas($key, $cmp, $val, $xt, %opts, $cb->($ret));
sub cas {
	my $cb = pop();
	my ($self, $key, $cmp, $val, $xt, %opts) = @_;

	my $db = exists($opts{database}) ? delete($opts{database}) : $self->{database};

	$self->call(
		'cas',
		{
			key => $key,
			(defined($cmp) ? (oval => $cmp) : ()),
			(defined($val) ? (nval => $val) : ()),
			(defined($xt)  ? (xt => $xt)    : ()),
			(defined($db)  ? (DB => $db)    : ()),
		},
		%opts,
		fetch_ret_cb($cb)
	);
}

# $kt->remove($key, $cb->($ret));
# $kt->remove($key, %opts, $cb->($ret));
sub remove {
	my $cb = pop();
	my ($self, $key, %opts) = @_;

	my $db = exists($opts{database}) ? delete($opts{database}) : $self->{database};

	$self->call(
		'remove',
		{key => $key, (defined($db) ? (DB => $db) : ())},
		%opts,
		fetch_ret_cb($cb)
	);
}

sub _get {
	my $cb = pop();
	my ($self, $proc, $key, %opts) = @_;

	my $db = exists($opts{database}) ? delete($opts{database}) : $self->{database};

	$self->call(
		$proc,
		{key => $key, (defined($db) ? (DB => $db) : ())},
		%opts,
		fetch_val_cb('value', 'xt', $cb)
	);
}

# $kt->get($key, $cb->($val, $xt));
# $kt->get($key, %opts, $cb->($val, $xt));
sub get { shift()->_get('get', @_); }

# $kt->check($key, $cb->($size, $xt));
# $kt->check($key, %opts, $cb->($size, $xt));
sub check {
	my $cb = pop();
	my ($self, $key, %opts) = @_;

	my $db = exists($opts{database}) ? delete($opts{database}) : $self->{database};

	$self->call(
		'check',
		{key => $key, (defined($db) ? (DB => $db) : ())},
		%opts,
		fetch_val_cb('vsiz', 'xt', $cb)
	);
}

# $kt->seize($key, $cb->($size, $xt));
# $kt->seize($key, %opts, $cb->($size, $xt));
sub seize { shift()->_get('seize', @_); }

# $kt->set_bulk(\%vals, $cb->($num));
# $kt->set_bulk(\%vals, $xt, $cb->($num));
# $kt->set_bulk(\%vals, $xt, %opts, $cb->($num));
sub set_bulk {
	my $cb = pop();
	my ($self, $vals, $xt, %opts) = @_;

	my $db   = exists($opts{database}) ? delete($opts{database}) : $self->{database};
	my $atom = exists($opts{atomic})   ? delete($opts{atomic}) // 1 : undef;

	$self->call(
		'set_bulk',
		{
			ref($vals) eq 'HASH' ? map { ('_' . $_ => $vals->{$_}) } keys(%$vals) : (),
			(defined($xt)   ? (xt => $xt)     : ()),
			(defined($db)   ? (DB => $db)     : ()),
			(defined($atom) ? (atomic => '1') : ()),
		},
		%opts,
		fetch_val_cb('num', $cb)
	);
}

# $kt->remove_bulk(\@keys, $cb->($num));
# $kt->remove_bulk(\@keys, %opts, $cb->($num));
sub remove_bulk {
	my $cb = pop();
	my ($self, $keys, %opts) = @_;

	my $db   = exists($opts{database}) ? delete($opts{database}) : $self->{database};
	my $atom = exists($opts{atomic})   ? delete($opts{atomic}) // 1 : undef;

	$self->call(
		'remove_bulk',
		{
			ref($keys) eq 'ARRAY' ? map { ('_' . $_ => '') } @$keys : (),
			(defined($db)   ? (DB => $db)     : ()),
			(defined($atom) ? (atomic => '1') : ()),
		},
		%opts,
		fetch_val_cb('num', $cb)
	);
}

# $kt->get_bulk(\@keys, $cb->(\%vals, $num));
# $kt->get_bulk(\@keys, %opts, $cb->(\%vals, $num));
sub get_bulk {
	my $cb = pop();
	my ($self, $keys, %opts) = @_;

	my $db   = exists($opts{database}) ? delete($opts{database}) : $self->{database};
	my $atom = exists($opts{atomic})   ? delete($opts{atomic}) // 1 : undef;

	$self->call(
		'get_bulk',
		{
			ref($keys) eq 'ARRAY' ? map { ('_' . $_ => '') } @$keys : (),
			(defined($db)   ? (DB => $db)     : ()),
			(defined($atom) ? (atomic => '1') : ()),
		},
		%opts,
		fetch_vals_cb($cb)
	);
}

# $kt->vacuum($cb->($ret));
# $kt->vacuum($step, $cb->($ret));
# $kt->vacuum($step, %opts, $cb->($ret));
sub vacuum {
	my $cb = pop();
	my ($self, $step, %opts) = @_;

	my $db = exists($opts{database}) ? delete($opts{database}) : $self->{database};

	$self->call(
		'vacuum',
		{
			(defined($db)   ? (DB => $db)     : ()),
			(defined($step) ? (step => $step) : ()),
		},
		%opts,
		fetch_ret_cb($cb)
	);
}

sub _match {
	my $cb = pop();
	my ($self, $proc, $test, $max, %opts) = @_;

	my $db = exists($opts{database}) ? delete($opts{database}) : $self->{database};

	$self->call(
		$proc,
		{
			prefix => $test,
			(defined($db)  ? (DB => $db)   : ()),
			(defined($max) ? (max => $max) : ()),
		},
		%opts,
		fetch_vals_cb($cb)
	);
}

# $kt->match_prefix($pref, $cb->(\%vals, $num));
# $kt->match_prefix($pref, $max, $cb->(\%vals, $num));
# $kt->match_prefix($prex, $max, %opts, $cb->(\%vals, $num));
sub match_prefix { shift()->_match('match_prefix', @_); }

# $kt->match_regex($regex, $cb->(\%vals, $num));
# $kt->match_regex($regex, $max, $cb->(\%vals, $num));
# $kt->match_regex($regex, $max, %opts, $cb->(\%vals, $num));
sub match_regex { shift()->_match('match_regex', @_); }

# $kt->match_similar($orig, $cb->(\%vals, $num));
# $kt->match_similar($orig, $range, $cb->(\%vals, $num));
# $kt->match_similar($orig, $range, $max, $cb->(\%vals, $num));
# $kt->match_similar($orig, $range, $max, %opts, $cb->(\%vals, $num));
sub match_similar {
	my $cb = pop();
	my ($self, $test, $range, $max, %opts) = @_;

	my $db  = exists($opts{database}) ? delete($opts{database}) : $self->{database};
	my $utf = exists($opts{utf8}) ? delete($opts{utf8}) // 1 : undef;

	$self->call(
		'match_similar',
		{
			origin => $test,
			(defined($db)    ? (DB => $db)       : ()),
			(defined($range) ? (range => $range) : ()),
			(defined($max)   ? (max => $max)     : ()),
			(defined($utf)   ? (utf => '1')      : ()),
		},
		%opts,
		fetch_vals_cb($cb)
	);
}

# $kt->create_cursor($id, $cb->($cur));
# $kt->create_cursor($id, %opts, $cb->($cur));
sub create_cursor {
	my $cb = pop();
	my ($self, $id, %opts) = @_;

	my $db = exists($opts{database}) ? delete($opts{database}) : $self->{database};

	require AnyEvent::KyotoTycoon::Cursor;

	$cb->(AnyEvent::KyotoTycoon::Cursor->new(client => $self, identifier => $id, database => $db));
}

# $kt->delete_cursor($cur, $cb->($ret));
# $kt->delete_cursor($cur, %opts, $cb->($ret));
sub delete_cursor {
	my $cb = pop();
	my (undef, $cur, %opts) = @_;

	$cur->delete(%opts, $cb);
}

sub call {
	my $cb = pop();
	my ($self, $proc, $data, %opts) = @_;

	my $enc  = exists($opts{encoding}) ? $opts{encoding} : $self->{encoding};
	my $body = encode_tsv($data, $enc);

	$self->_request(
		'http://' . $self->{server} . '/rpc/' . $proc,
		$body,
		$enc,
		sub {
			unless (@_) {
				$cb->();
				return;
			}

			my ($code, $body, $enc) = @_;
			my $data = decode_tsv($body, $enc);

			unless ($data) {
				AE::log(error => 'Body decoding failed');
				$cb->();
				return;
			}
			unless ($code == 200) {
				if (exists($data->{ERROR})) {
					AE::log(error => 'Server error raised: ' . $data->{ERROR});
				}
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

			my $code = $hdrs->{Status};

			unless ($code == 200) {
				AE::log(error => 'Request failed: ' . $code . ' (' . $hdrs->{Reason} . ')');
				unless ($code == 450) {
					$cb->();
					return;
				}
			}

			my $enc = check_encoding($hdrs->{'content-type'});

			unless (defined($enc)) {
				AE::log(error => 'Unknown content type received: ' . ($hdrs->{'content-type'} // 'undef'));
				$cb->();
				return;
			}

			$cb->($code, $body, $enc);
			return;
		}
	);
}


1;


__END__

=head1 NAME

AnyEvent::KyotoTycoon - non-bloking client for KyotoTycoon server.

=head1 SYNOPSIS

    use AnyEvent::KyotoTycoon;

    my $kt = AnyEvent::KyotoTycoon->new(server => '127.0.0.1:1978');
    my $cv = AE::cv;

    $kt->set(foo => 'bar', sub {
        if (@_) {
            $kt->get('foo', sub {
                $cv->send($_[0]);
            });
        }
        else {
            $cv->send();
        }
    });
    say($cv->recv());

=head1 DESCRIPTION

AnyEvent::KyotoTycoon provides non-blocking KyotoTycoon API version 1 using
L<AnyEvent::HTTP>.

See the L<KyotoTycoon protocol version 1 specification|http://fallabs.com/kyototycoon/spex.html#protocol>
for greater detail.

=head1 METHODS

All methods except the constructor C<new> correspond KyotoTycoon protocol
procedures. These methods take a code reference (callback) as the last argument.
If the request to the server is successful, then this callback will be called
with some arguments that depends of the type of procedure. If there is protocol
or communication error, then callback will be called with no arguments.

=head2 new

    my $kt = AnyEvent::UserAgent->new(%args);

Constructor for the client. Any of the attributes with accessor methods
described below may be passed to the constructor as key-value pairs.

=over

=item server

Host name and port number of server machine. Default is C<127.0.0.1:1978>.

=item database

Name or identifier of database. Default is undefined.

=item encoding

Encoding for TSV-RPC call. Following encodings are available: C<B> - Base64,
C<Q> - Quoted-Printable, C<U> - URI escape, or false value - no encoding.
Default is no encoding.

=item timeout

Timeout value for each request in seconds. Default is 1 second.

=back

=head2 void

    $kt->void($cb->($ret));

Does nothing, just for testing. Callback will be triggered with true value on
success.

=head2 echo

=head2 report

=head2 play_script

=head2 status

=head2 clear

=head2 synchronize

=head2 set

=head2 add

=head2 replace

=head2 append

=head2 increment

=head2 increment_double

=head2 cas

=head2 remove

=head2 get

=head2 check

=head2 seize

=head2 set_bulk

=head2 remove_bulk

=head2 get_bulk

=head2 vacuum

=head2 match_prefix

=head2 match_regex

=head2 match_similar

=head1 SEE ALSO

L<AnyEvent::HTTP>,
L<Cache::KyotoTycoon>,
L<KyotoTycoon protocol version 1 specification|http://fallabs.com/kyototycoon/spex.html#protocol>.

=head1 SUPPORT

=over 4

=item Repository

L<http://github.com/AdCampRu/anyevent-kyototycoon>

=item Bug tracker

L<http://github.com/AdCampRu/anyevent-kyototycoon/issues>

=back

=head1 AUTHOR

Denis Ibaev C<dionys@cpan.org> for AdCamp.ru.

=head1 LICENSE

This library is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

See L<http://dev.perl.org/licenses/> for more information.

=cut
