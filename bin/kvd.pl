#!/usr/bin/perl

use strict;
use lib ('/Users/joshua/projects/sisyphus/lib');
use lib ('/Users/joshua/projects/mykeyv/lib');
use lib ('/home/joshua/projects/sisyphus/lib');
use lib ('/home/joshua/projects/mykeyv/lib');

use File::Lockfile;
use Proc::Daemon;
use Sys::Hostname;
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
require $confPath;
my $config = $Config::config;

# lockfile
my $lockfile = File::Lockfile->new($config->{dname}, "/tmp");
my $pid = $lockfile->check();
if ($pid) { print "Lockfile detected for daemon " . $config->{dname} . ", pid $pid\n"; exit;}


# background, if wanted.
$config->{daemonize} && Proc::Daemon::Init; 

my $log = Sislog->new({use_syslog=>1, facility=>$config->{dname}});
$log->open();

$lockfile->write;

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

$lockfile->remove;

END {
	if ($log and ($! or $@)) {
		my $errm = "kvd ended with error: $! $@";
		$log->log($errm, time);
	}
}
