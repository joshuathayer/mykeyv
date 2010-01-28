package Mykeyv::MyKVStubClient;

# MyKV client library.

#use lib ("/Users/joshua/projects/sisyphus/lib/");
use strict;
use AnyEvent::Strict;
use Sisyphus::Connector;
use Sislog;
use JSON "-convert_blessed_universally";
use Data::Dumper;

use Set::ConsistentHash;
use String::CRC32;
use Scalar::Util qw/ weaken /;

use Test::MockObject;

my $mock = Test::MockObject->new();
$mock->fake_module(
    'Mykeyv::MyKVClient'

# OH HAI THIS IS IMPLEMENTED AS A SINGLETON
my $instance = undef;

sub new {
    my ($class, $in) = @_;

    # if we have been here before ($instance set),
    # and we are not passing in $in (new connection config),
    # return the existing instance.
    if ($instance and not $in) { return $instance };

    # if we have an instance, we're going to want to operate
    # upon it (open connections and the like).
    # if we don't have an instance, we're going to create a 
    # blessed ref, stash it away, and then operate on $self
    my $self;
    if ($instance) {
        $self = $instance;
    } else {
        $self = {};

        bless $self, $class;
        $instance = $self;
    }

    unless ($in) { return $self; };

    return $self;
}

# init the consistent hash object
sub prep_set {
    my ($self, $set, $cluster) = @_;
    return undef;
}

# connect to every kvd server. blocks!
sub makeConnections {
    my $self = shift;
    my $set_name = shift;
    my $set;    
    my $cluster;

    return undef;
}

sub get {
    my ($self, $key, $cb) = @_;

    weaken $self;

    if ($self->{stuff}->{$key}) { $cb->( $self->{stuff}->{$key} ); }
    else { $cb->(undef); }

}

# list- list all keys across the entire cluster
# really bad idea to do this in anything close to a production machine
sub list {
	my ($self, $cb) = @_;

    # unimplemented in stub, so far. look at real client if you want to implement this
    $cb->({'haha'=>["list method unimplemented"]});

}

sub rehash {
    my ($self, $cb) = @_;

    # unimplemented in stub
    $cb->(undef);
}

# see readme. run code on remote servers to modify records in the db.
sub update {
    my ($self, $keys, $code, $args, $cb) = @_;

    # unimplemented in stub
    $cb->(undef);
}

sub set {
    my ($self, $key, $val, $cb) = @_;
    $self->{stuff}->{$key}=$val;
    $cb->();
}

1;
