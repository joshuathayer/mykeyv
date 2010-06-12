#!/usr/bin/perl

# delete something

use strict;
use Mykeyv::MyKVClient;
use JSON;
use Data::Dumper;

BEGIN {
	if ($#ARGV < 1) {
		print "Usage: $0 PATH_TO_CONFFILE key_to_delete\n";
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

my $key = $ARGV[1];

my $cv = AnyEvent->condvar;
$kvc->delete($key, sub {
	my  $r = shift;
	print "delete done:\n";
	print Dumper $r;
	$cv->send;
});
$cv->recv;

