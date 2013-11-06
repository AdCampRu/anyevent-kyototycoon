package AnyEvent::KyotoTycoon::Cursor;

use strict;
use warnings;


sub new {
	my ($proto, %args) = @_;

	return bless(
		{
			client     => $args{client},
			identifier => $args{identifier},
		},
		ref($proto) || $proto
	);
}

sub _jump {
	my $cb = pop();
	my ($self, $proc, $key, %opts) = @_;

	my $db = exists($opts{database}) ? delete($opts{database}) : $self->{client}->database;

	$self->{client}->call(
		$proc,
		{
			CUR => $self->{identifier},
			(defined($db)  ? (DB => $db)   : ()),
			(defined($key) ? (key => $key) : ()),
		},
		%opts,
		sub {
			$cb->($_[0] ? 1 : ());
		}
	);
}

# $cur->jump($cb->($ret));
# $cur->jump($key, $cb->($ret));
# $cur->jump($key, %opts, $cb->($ret));
sub jump { shift()->_jump('cur_jump', @_); }

# $cur->jump_back($cb->($ret));
# $cur->jump_back($key, $cb->($ret));
# $cur->jump_back($key, %opts, $cb->($ret));
sub jump_back { shift()->_jump('cur_jump_back', @_); }

sub _step {
	my $cb = pop();
	my ($self, $proc, %opts) = @_;

	$self->{client}->call(
		$proc,
		{CUR => $self->{identifier}},
		%opts,
		sub {
			$cb->($_[0] ? 1 : ());
		}
	);
}

# $cur->step($cb->($ret));
# $cur->step(%opts, $cb->($ret));
sub step { shift()->_step('cur_step', @_); }

# $cur->step_back($cb->($ret));
# $cur->step_back(%opts, $cb->($ret));
sub step_back { shift()->_step('cur_step_back', @_); }

# $cur->set_value($val, $cb->($ret));
# $cur->set_value($val, $xt, $cb->($ret));
# $cur->set_value($val, $xt, $step, $cb->($ret));
# $cur->set_value($val, $xt, $step, %opts, $cb->($ret));
sub set_value {
	my $cb = pop();
	my ($self, $val, $xt, $step, %opts) = @_;

	$self->{client}->call(
		'cur_set_value',
		{
			CUR => $self->{identifier}
			(defined($xt)   ? (xt => $xt)  : ()),
			(defined($step) ? (step => '') : ()),
		},
		%opts,
		sub {
			$cb->($_[0] ? 1 : ());
		}
	);
}

# $cur->remove($cb->($ret));
# $cur->remove(%opts, $cb->($ret));
sub remove {
	my $cb = pop();
	my ($self, %opts) = @_;

	$self->{client}->call(
		'cur_remove',
		{CUR => $self->{identifier}},
		%opts,
		sub {
			$cb->($_[0] ? 1 : ());
		}
	);
}

# $cur->get_key($cb->($ret));
# $cur->get_key($step, %opts, $cb->($ret));
sub get_key {
	my $cb = pop();
	my ($self, $step, %opts) = @_;

	$self->{client}->call(
		'cur_get_key',
		{
			CUR => $self->{identifier}
			(defined($step) ? (step => '') : ()),
		},
		%opts,
		sub {
			$cb->($_[0] ? $_[0]{key} : ());
		}
	);
}

# $cur->get_value($cb->($ret));
# $cur->get_value($step, %opts, $cb->($ret));
sub get_value {
	my $cb = pop();
	my ($self, $step, %opts) = @_;

	$self->{client}->call(
		'cur_get_value',
		{
			CUR => $self->{identifier}
			(defined($step) ? (step => '') : ()),
		},
		%opts,
		sub {
			$cb->($_[0] ? $_[0]{value} : ());
		}
	);
}

# $cur->get($cb->($ret));
# $cur->get($step, %opts, $cb->($ret));
sub get {
	my $cb = pop();
	my ($self, $step, %opts) = @_;

	$self->{client}->call(
		'cur_get',
		{
			CUR => $self->{identifier}
			(defined($step) ? (step => '') : ()),
		},
		%opts,
		sub {
			$cb->($_[0] ? ($_[0]{key}, $_[0]{value}, $_[0]{xt}) : ());
		}
	);
}

# $cur->seize($cb->($ret));
# $cur->seize(%opts, $cb->($ret));
sub seize {
	my $cb = pop();
	my ($self, %opts) = @_;

	$self->{client}->call(
		'cur_seize',
		{CUR => $self->{identifier}},
		%opts,
		sub {
			$cb->($_[0] ? ($_[0]{key}, $_[0]{value}, $_[0]{xt}) : ());
		}
	);
}

# $cur->delete($cb->($ret));
# $cur->delete(%opts, $cb->($ret));
sub delete {
	my $cb = pop();
	my ($self, %opts) = @_;

	$self->{client}->call(
		'cur_delete',
		{CUR => $self->{identifier}},
		%opts,
		sub {
			$cb->($_[0] ? 1 : ());
		}
	);
}


1;
