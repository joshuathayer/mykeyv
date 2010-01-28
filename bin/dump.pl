#!/usr/bin/perl

# listr evenything in the cluster!

use Mykeyv::MyKVClient;
use JSON;
use Data::Dumper;

BEGIN {
	if ($#ARGV < 1) {
		print "Usage: $0 PATH_TO_CONFFILE KEY\n";
		exit;
	}
}

require $ARGV[0];
my $cluster = $Config::cluster;
my $pending_cluster = $Config::pending_cluster;
my $cluster_state = $Config::cluster_state;

my $key = $ARGV[1];

my $kvc = Mykeyv::MyKVClient->new({
	cluster => $cluster,
	cluster_state => $cluster_state,
});

my $cv = AnyEvent->condvar;
$kvc->get($key, sub {
	my $item = shift;
	print Dumper $item;
	print "dump done\n";
	$cv->send;
});
$cv->recv;

