#!/usr/bin/perl

use strict;

use Sisyphus::Listener;
use Sisyphus::Proto::HTTP;
use AnyEvent::Strict;
use Mykeyv::MyKVClient;
use ExplorerApplication;

my $confPath;

BEGIN {
    if ($#ARGV < 0) {
        print "Usage: $0 PATH_TO_CONFFILE\n";
        exit;
    }

    $confPath = $ARGV[0];
    # this *IS* Config
    require $confPath;
}

my $cluster = $Config::cluster;
my $pending_cluster = $Config::pending_cluster;
my $cluster_state = $Config::cluster_state;

# kvc
my $kvc = Mykeyv::MyKVClient->new({
   cluster => $cluster,
   cluster_state => $cluster_state,
});

# logging
my $applog = Sislog->new({use_syslog=>1, facility=>$Config::applog}); # mykeyv-http
my $httplog = Sislog->new({use_syslog=>1, facility=>$Config::httplog}); # mykeyv-app
$applog->open();
$httplog->open();

my $listener = new Sisyphus::Listener;

$listener->{port} = $Config::port;
$listener->{ip} = $Config::ip;
$listener->{protocol} = "Sisyphus::Proto::HTTP";
$listener->{application} = ExplorerApplication->new(
        $kvc,
        $httplog,
        $applog,
);
$listener->listen();

AnyEvent->condvar->recv;

