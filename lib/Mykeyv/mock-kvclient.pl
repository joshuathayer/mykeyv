# mock the MyKV client library.

use strict;
use Data::Dumper;
use Scalar::Util qw/ weaken /;
use Test::MockObject;

# OH HAI THIS IS IMPLEMENTED AS A SINGLETON
my $instance = undef;

my $mock = Test::MockObject->new();

$mock->fake_module( 'Mykeyv::MyKVClient',
    'new' => sub {
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
    },
    
    # init the consistent hash object
    'prep_set' => sub {
        my ($self, $set, $cluster) = @_;
        return undef;
    },
    
    # connect to every kvd server. 
    'makeConnections' => sub {
        my $self = shift;
        my $set_name = shift;
        my $set;    
        my $cluster;
    
        return undef;
    },
    
    'get' => sub {
        my ($self, $key, $cb) = @_;
    
        print "MOCK OBJECT USING GET\n";
    
        if ($self->{stuff}->{$key}) { $cb->( $self->{stuff}->{$key} ); }
        else { $cb->(undef); }
    
    },
    
    # list- list all keys across the entire cluster
    # really bad idea to do this in anything close to a production machine
    'list' => sub {
    	my ($self, $cb) = @_;
    
        # unimplemented in stub, so far. look at real client if you want to implement this
        $cb->({'haha'=>["list method unimplemented"]});
    
    },
    
    'rehash' => sub {
        my ($self, $cb) = @_;
    
        # unimplemented in stub
        $cb->(undef);
    },
    
    # see readme. run code on remote servers to modify records in the db.
    'update' => sub {
        my ($self, $keys, $code, $args, $cb) = @_;
    
        # unimplemented in stub
        $cb->(undef);
    },
    
    'set' => sub {
        my ($self, $key, $val, $cb) = @_;
        print "MOCK OBJECT USING SET\n";
        $self->{stuff}->{$key}=$val;
        $cb->();
    },
    
);
    
1;
