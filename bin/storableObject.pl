#!/usr/bin/perl
# client using storableobject parent
package FakeUser;
use base 'Mykeyv::StorableObject';

sub new {
        my ($class) = @_;

	my $self = $class->SUPER::new();

	# hints to mykeyv regarding how to store instances of this object
        $self->{'_mykv_storable_make_key'} = sub { return "fakeuser_object_".$self->{first}; };
        $self->{'_mykv_storable_object_vars'} = ['first','last','friends'];

	# users have a first name, a last name, and a list of friends
        $self->{'first'} = '';
        $self->{'last'} = '';
        $self->{'friends'} = [];

	bless($self, $class);
	return $self;
}

sub mykv_append {
	my ($self, $args) = @_;

	unshift(@{$self->{friends}}, $args->{name});

	return 1;
}

package main;

use Mykeyv::MyKVClient;
use strict;
use Data::Dumper;

BEGIN {
	if ($#ARGV < 0) {
		print "Usage: $0 PATH_TO_CONFFILE\n";
		exit;
	}
}

require $ARGV[0];
my $cluster = $Config::cluster;
my $cluster_state = $Config::cluster_state;
my $pending_cluster = $Config::pending_cluster;

my $kvc = Mykeyv::MyKVClient->new({
	cluster => $cluster,
	cluster_state => $cluster_state,
});

# create and store a jerry object.
my $u = FakeUser->new();
$u->{'first'} = "jerry";
$u->{'last'} = "garcia";

my $cv = AnyEvent->condvar;
$u->mykvStore(sub{$cv->send});
$cv->recv;

print "stored new jerry object.\n";

# ...later (or in another piece of code)...

# add joshua to the list of jerry's friends. 
# the 'both' modifies the list both locally *and* on the remote db server
my $cv = AnyEvent->condvar;
$u->mykv_append_both({name=>"joshua"}, sub {$cv->send});
$cv->recv;

print Dumper $u;
