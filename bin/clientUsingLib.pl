#!/usr/bin/perl
# client using client lib

use lib ("/Users/joshua/projects/sisyphus/lib/");
use lib ("/Users/joshua/projects/mykeyv/lib/");

use MyKVClient;
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

my $kvc = MyKVClient->new({
	cluster=>$cluster,
});

my $cv = AnyEvent->condvar;
$kvc->set("joshua", {
		age => 34,
		first => "joshua",
		last => "thayer",
		parties => ["brithday", "bastille", "cinco de mayo", "wine and cheese ride"],
	}, sub {
		print "ok properly set joshua object. going to get it now!\n";
		$cv->send;
	}); 
$cv->recv;

my $cv = AnyEvent->condvar;
$kvc->get("joshua", sub {
	my $r = shift;
	print "age is " . $r->{data}->{age} . "\n";
	$cv->send;
});
$cv->recv;

