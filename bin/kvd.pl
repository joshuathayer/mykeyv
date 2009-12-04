#!/usr/bin/perl

use strict;

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

my $cluster = $Config::cluster;
my $pending_cluster = $Config::pending_cluster;
my $cluster_state = $Config::cluster_state;

# background, if wanted.
unless(check_pid_file("/var/run/" . $config->{dname} . ".pid")) { print $config->{dname} . " already running- aborting"; exit; }
$config->{daemonize} && daemonize('nobody','nogroup',"/var/run/".$config->{dname}.".pid"); 

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
	$cluster,
	$pending_cluster,
	$cluster_state,
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
