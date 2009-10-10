#!/usr/bin/perl
# client using client lib

use lib ("/Users/joshua/projects/sisyphus/lib/");
use lib ("/Users/joshua/projects/mykeyv/lib/");
use lib ("/home/joshua/projects/sisyphus/lib/");
use lib ("/home/joshua/projects/mykeyv/lib/");

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

#my $cv = AnyEvent->condvar;
#foreach my $key (qw/ringo john joshua jones frank dweezil moonunit dave jimmy miles jerry phil bob bill zakhir/) {
#	$cv->begin;
#	$kvc->set($key, {
#			age => int(rand() * 100),
#			first => $key,
#			last => "LeTest",
#			parties => ["brithday", "bastille", "cinco de mayo", "wine and cheese ride"],
#			amICool => "yes",
#		}, sub {
#			print "ok properly set $key object.\n";
#			$cv->end;
#		}); 
#}
#$cv->recv;

my $cv = AnyEvent->condvar;
$kvc->get("bob", sub {
	my $r = shift;
	print "age is " . $r->{data}->{age} . "\n";
	print "last name is " . $r->{data}->{last} . "\n";
	print "cool? " . $r->{data}->{amICool} . "\n";
	$cv->send;
});
$cv->recv;

