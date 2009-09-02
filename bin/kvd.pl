#!/usr/bin/perl

use strict;
use lib ('/home/joshua/projects/sisyphus/lib');
use lib ('/home/joshua/projects/mykeyv/lib');

use Sisyphus::Listener;
use Sisyphus::Proto::Trivial;
use AnyEvent::Strict;
use MyKVApp;

BEGIN {
	if ($#ARGV < 0) {
		print "Usage: $0 PATH_TO_CONFFILE\n";
		exit;
	}
}

my $confPath = $ARGV[0];
print "confpath $confPath\n";
require $confPath;

my $config = $Config::config;
print Dumper $Config::config;

my $listener = new Sisyphus::Listener;

$listener->{port} = $config->{port}; # 8889;
$listener->{ip} = $config->{ip}; # "127.0.0.1";
$listener->{protocol} = "Sisyphus::Proto::Trivial";
$listener->{application} = MyKVApp->new(
	$config->{dbip},
	$config->{dbport},
	$config->{dbuser},
	$config->{dbpw},
	$config->{dbdb},
);
$listener->{use_push_write} = 0;
$listener->listen();

AnyEvent->condvar->recv;

