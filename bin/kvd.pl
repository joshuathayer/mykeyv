#!/usr/bin/perl

use strict;
use lib ('/home/joshua/projects/sisyphus/lib');
use lib ('/home/joshua/projects/mykeyv/lib');

use Sisyphus::Listener;
use Sisyphus::Proto::Trivial;
use AnyEvent::Strict;
use MyKVApp;

my $listener = new Sisyphus::Listener;

$listener->{port} = 8889;
$listener->{ip} = "127.0.0.1";
$listener->{protocol} = "Sisyphus::Proto::Trivial";
$listener->{application} = MyKVApp->new();
$listener->{use_push_write} = 0;
$listener->listen();

AnyEvent->condvar->recv;

