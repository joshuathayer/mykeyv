#!/usr/bin/perl
# client using storableobject parent
package FakeUser;
use base 'Mykeyv::StorableObject';

sub new {
        my ($class) = @_;

	my $self = $class->SUPER::new();

        $self->{'_mykv_storable_make_key'} = sub { return "fakeuser_object_".$self->{first}; };
        $self->{'_mykv_storable_object_vars'} = ['first','last','friends'];
        $self->{'first'} = '';
        $self->{'last'} = '';

        $self->{'friends'} = [];

	bless($self, $class);
	return $self;
}

sub mykv_remote_append {
	my ($self, $args) = @_;

	print "wow wtf! do you see me?\n";

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

print "cluster state $cluster_state\n";
print Dumper $cluster;

my $kvc = Mykeyv::MyKVClient->new({
	cluster => $cluster,
	pending_cluster => $pending_cluster,
	cluster_state => $cluster_state,
});

my $u = FakeUser->new();
$u->{'first'} = "jerry";
$u->{'last'} = "garcia";

my $cv = AnyEvent->condvar;
$u->mykvStore(sub{$cv->send});
$cv->recv;

my $cv = AnyEvent->condvar;
my $w = FakeUser->new();
$w->{'first'} = "jerry";
$w->mykvRestore(sub{$cv->send});
$cv->recv;
print "jerry's last name is $w->{last}\n";      # prints "jerry's last name is garcia"
my $cv = AnyEvent->condvar;
$w->mykv_remote_append({name=>"joshua"}, sub {$cv->send});
$cv->recv;
