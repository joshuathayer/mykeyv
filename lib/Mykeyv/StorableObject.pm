package Mykeyv::StorableObject;
use strict;
use Mykeyv::MyKVClient;
use Data::Dumper;
use B::Deparse;

# a parent object that allows its children to store themselves

my $kvc = Mykeyv::MyKVClient->new();    # singleton in MyKVClient

sub new {
    my $class = shift;

    my $deparse = B::Deparse->new("-p", "-sC");

    my $self = {};

    bless ($self, $class);

    my @meths = methods($self);

    # munge things into remote and remote+local versions
    foreach my $m (@meths) {
        if ($m =~ /^mykv_.*/) {
            no strict 'refs';
            my $name = $class. "::";

            # we only want to do this once
            next if $name->{"__mykv_scalar_$m"};
            next if ($m =~ /(both|remote)/);

            # basically, copy the sub by decompiling and recompiling it.
            # stick it back in the package as mykv_local_$functionName
            my $ref = $self->can($m);
            my $scalar = $deparse->coderef2text($ref);
            #print "SCALAR $scalar\n";
            print "StorableObject:new compile $m\n";
            my $runme = "my \$s = sub { $scalar }; return \$s;";
            my $sub;
            $sub = eval $runme;

            # we stash aside the source
            $name->{"__mykv_scalar_$m"} = $scalar;

            # we rename the local copy
            #$name->{"mykv_local_$m"} = $sub;
            # no. let's keep the local copy pristene

            # we create a remote version of the sub
            #$name->{$m."_remote"} = sub {
            *{"$name${m}_remote"} = sub {
                my ($self, $args, $cb) = @_;
                my $token = $self->{'_computed_mykv_key'} || $self->{'_mykv_storable_make_key'}->();
                       $self->{'_computed_mykv_key'} = $token;

                print "i would update $token with:\n";
                print Dumper $args;
                print "using code\n" . $name->{"__mykv_scalar_".$m} . "\n";

                # so. 
                # remote($keys, $code, $args, $cb);
                # XXX future: cache this code on the server, to avoid an eval 
                $kvc->update([$token], $name->{"__mykv_scalar_$m"}, $args, $cb);

            };

            # and we create a 'both' version, which runs the local sub and the remote sub
            *{"$name${m}_both"} = sub {
                my ($self, $args, $cb) = @_;
    
                # call local first
                #$name->{"mykv_local_$m"}->($self, $args);
                #$self->{$m}->($self, $args);
                *{$name.$m}->($self, $args);
    
                # and then call remote
                #$self->{ "${m}_remote" }->($self, $args, $cb);
                *{"$name${m}_remote"}->($self, $args, $cb);
            };
            
        }
    }

    return $self;
}


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
    my $n = 'StorableObject:mykvRestore';

    # step zero- tell me what's my name
    my $token = $self->{'_computed_mykv_key'} || $self->{'_mykv_storable_make_key'}->();
    $self->{'_computed_mykv_key'} = $token;
    print "$n using token $token in Restore!\n";

    # hit up the DB
    $kvc->get($token, sub {
        # copy over params
        my $dat = shift;
        unless ($dat) { print "$n:kvc_cb restore failure!\n"; $cb->(undef); return; }
        foreach my $k (keys (%{$dat->{data}})) {
            $self->{ $k } = $dat->{data}->{ $k };
        }
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


#sub remote {
#    my ($self, $sub) = @_;
#
#    $self = ref $self || $self;
#
#    # wow!
#    # http://www.perlmonks.org/?node_id=62737
#    # http://perldoc.perl.org/5.8.8/B/Deparse.html
#    # http://dev.perl.org/perl6/rfc/335.html
#
#    my @foo = methods($self, 'all');
#
#    my $deparse = B::Deparse->new("-p", "-sC");
#
#    #        my ($self, $keys, $code, $args, $cb) = @_;
#
#
#    #foreach my $f (@foo) {
#    #    if ($f =~ /mykv_/) {
#    #        my $ref = $self->can($f);
#    #        my $scalar = $deparse->coderef2text($ref);
#
#    #    }
#    #}
#
#    my $ref = $self->can($sub);
#    my $scalar = $deparse->coderef2text($ref);
#    #$kvc->update($XXX, $scalar, $XXX, $cb);
#
#}
