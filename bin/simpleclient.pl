#!/usr/bin/perl
# client using client lib

use Mykeyv::MyKVClient;
use JSON;
use Data::Dumper;

BEGIN {
	if ($#ARGV < 0) {
		print "Usage: $0 PATH_TO_CONFFILE\n";
		exit;
	}
}

require $ARGV[0];
my $cluster = $Config::cluster;
my $pending_cluster = $Config::pending_cluster;
my $cluster_state = $Config::cluster_state;

my $kvc = Mykeyv::MyKVClient->new({
	cluster => $cluster,
	pending_cluster => $pending_cluster,
	cluster_state => $cluster_state,
});

print "hi.\n";

my $cv = AnyEvent->condvar;
$kvc->get("jerry", sub {
	print "here here yes here\n";
	my $r = shift;
	print "age is " . $r->{data}->{age} . "\n";
	print "first name is " . $r->{data}->{first} . "\n";
	print "last name is " . $r->{data}->{last} . "\n";
	print "cool? " . $r->{data}->{amICool} . "\n";
	$cv->send;
});
$cv->recv;

