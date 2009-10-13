#!/usr/bin/perl

use strict;
use lib ('/Users/joshua/projects/sisyphus/lib');
use lib ('/Users/joshua/projects/mykeyv/lib');
use lib ('/home/joshua/projects/sisyphus/lib');
use lib ('/home/joshua/projects/mykeyv/lib');



BEGIN {
	if ($#ARGV < 0) {
		print "Usage: $0 PATH_TO_CONFFILE\n";
		exit;
	}

}

use File::Lockfile;
use Sys::Hostname;
use Sisyphus::Listener;
use Sisyphus::Proto::Trivial;
use AnyEvent::Strict;
use MyKVApp;
use Net::Server::Daemonize qw ( daemonize check_pid_file );

my $config;
my $confPath = $ARGV[0];
require $confPath;
$config = $Config::config;

# background, if wanted.
unless(check_pid_file("/var/run/" . $config->{dname} . ".pid")) { print $config->{dname} . " already running- aborting"; exit; }
$config->{daemonize} && daemonize('nobody','nobody',"/var/run/".$config->{dname}.".pid"); 

my $log = Sislog->new({use_syslog=>1, facility=>$config->{dname}});
$log->open();

$log->log("keyvalue coming up");
my $listener = new Sisyphus::Listener;

$listener->{port} = $config->{port}; 
$listener->{ip} = $config->{ip}; 
$listener->{protocol} = "Sisyphus::Proto::Trivial";
$listener->{application} = MyKVApp->new(
	$config->{dbip},
	$config->{dbport},
	$config->{dbuser},
	$config->{dbpw},
	$config->{dbdb},
	$config->{dbtable},
	$log,
);
$listener->{use_push_write} = 0;
$listener->listen();

AnyEvent->condvar->recv;

# $lockfile->remove;

END {
	if ($log and ($! or $@)) {
		my $errm = "kvd ended with error: $! $@";
		$log->log($errm, time);
	}
}
