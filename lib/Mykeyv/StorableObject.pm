package Mykeyv::StorableObject;
use strict;
use Mykeyv::MyKVClient;
use Data::Dumper;

# a parent object that allows its children to store themselves

my $kvc = Mykeyv::MyKVClient->new();	# singleton in MyKVClient

sub mykvStore {
	my ($self, $cb) = @_;

	print Dumper $self;
	print Dumper $cb;

	# step zero- tell me what's my name
	my $token = $self->{'_computed_mykv_key'} || $self->{'_mykv_storable_make_key'}->();
	$self->{'_computed_mykv_key'} = $token;

	# step one- make a hash
	my $yep;
	foreach my $k (@{$self->{'_mykv_storable_object_vars'}}) {
		$yep->{ $k } = $self->{ $k };
	}

	# step two- put it on wax
	$kvc->set(
		$token,
		$yep,
		sub { "in mykvStore cb!!\n"; $cb->(); },
	);
}

sub mykvRestore {
	my ($self, $cb) = @_;

	# step zero- tell me what's my name
	my $token = $self->{'_computed_mykv_key'} || $self->{'_mykv_storable_make_key'}->();
	$self->{'_computed_mykv_key'} = $token;
	print "using token $token in Restore!\n";

	# hit up the DB
	$kvc->get($token, sub {

		# copy over params
		my $dat = shift;
		foreach my $k (keys (%{$dat->{data}})) {
			print "key $k!\n";
			$self->{ $k } = $dat->{data}->{ $k };
		}
		print Dumper $self;
		$cb->($self);
	});
}
