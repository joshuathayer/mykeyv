package Mykeyv::StorableObject;
use strict;
use Mykeyv::MyKVClient;
use Data::Dumper;
use B::Deparse;

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


sub methods {
	my ($class, $types) = @_;
	$class = ref $class || $class;
	$types ||= '';
	my %classes_seen;
	my %methods;
	my @class = ($class);

	no strict 'refs';
	while ($class = shift @class) {
		next if $classes_seen{$class}++;
		unshift @class, @{"${class}::ISA"} if $types eq 'all';
		# Based on methods_via() in perl5db.pl
		for my $method (grep {not /^[(_]/ and 
			defined &{${"${class}::"}{$_}}} 
			keys %{"${class}::"}) {

				#my $yeah = $class.'::'.$method;
				my $yeah = $method;
				$methods{$yeah} = wantarray ? undef : $class->can($method); 
		}
	}
      
	wantarray ? keys %methods : \%methods;
}


sub remote {
	my ($self, $sub) = @_;

	$self = ref $self || $self;

	# wow!
	# http://www.perlmonks.org/?node_id=62737
	# http://perldoc.perl.org/5.8.8/B/Deparse.html
	# http://dev.perl.org/perl6/rfc/335.html

	my @foo = methods($self, 'all');

	my $deparse = B::Deparse->new("-p", "-sC");

	foreach my $f (@foo) {
		if ($f =~ /mykv_/) {
			my $ref = $self->can($f);
			print $deparse->coderef2text($ref);
		}
	}

}
