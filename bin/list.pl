#!/usr/bin/perl

# listr evenything in the cluster!

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
	cluster_state => $cluster_state,
});

print "hi.\n";

my $cv = AnyEvent->condvar;
$kvc->list(sub {
	my $list = shift;
	print Dumper $list;
	print "list done\n";
	$cv->send;
});
$cv->recv;

