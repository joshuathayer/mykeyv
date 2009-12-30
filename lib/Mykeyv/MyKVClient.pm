package Mykeyv::MyKVClient;

# MyKV client library.

use lib ("/Users/joshua/projects/sisyphus/lib/");
use strict;
use AnyEvent::Strict;
use Sisyphus::Connector;
use Sislog;
use JSON "-convert_blessed_universally";
use Data::Dumper;

use Set::ConsistentHash;
use String::CRC32;
use Scalar::Util qw/ weaken /;

sub new {
	my $class = shift;
	my $in = shift;

	my $self = {
		set => undef,
		pending_set => undef,
		cluster => $in->{cluster},
		pending_cluster => $in->{pending_cluster},
		cluster_state => $in->{cluster_state},	# normal | pending | pending-write
		log => Sislog->new({use_syslog => 1, facility=>"MyKVClient"}),
		request_id => 0,
		data_callbacks => {},
	};

	$self->{log}->open();

	# bless the object now, so we can call methods on it
	bless $self, $class;

	$self->prep_set("set", $self->{cluster});

	$self->makeConnections();
	$self->{log}->log("under first makeConnections");

	# if we have are in a pending state, we want to prepare a hash object for that, too
	if (($self->{cluster_state} eq "pending") or ($self->{cluster_state} eq "pending-write")) {
		$self->{log}->log("in pending connection clause");
		$self->prep_set("pending_set", $self->{pending_cluster});
		$self->makeConnections("pending_set");	
		$self->{log}->log("under second makeConnections");
	}
	
	return $self;
}

# init the consistent hash object
sub prep_set {
	my ($self, $set, $cluster) = @_;

	$self->{log}->log("prep_set $set");

	# set is a ConsistentHash object.
	$self->{ $set } = Set::ConsistentHash->new,
	$self->{ $set }->set_hash_func(\&crc32);

	# cluster is an array of hashes.  each hash represents a kvd server.
	# we want to add each element of the cluster array into the
	# ConsistentHash, with even weights.
	my $i = 0; my $targets;
	foreach my $t (@{$cluster}) {
		$targets->{$i} = 1;
		$i += 1;
	}
	$self->{ $set }->modify_targets( %$targets );
}

# connect to every kvd server. blocks!
sub makeConnections {
	my $self = shift;
	my $set_name = shift;
	my $set;	
	my $cluster;

	if ($set_name eq "pending_set") { 
		$set = $self->{pending_set};
		$cluster = $self->{pending_cluster};
	} else {
		$set_name = "set";	# for reporting, below
		$set = $self->{set};
		$cluster = $self->{cluster};
	}

	foreach my $target ($set->targets()) {
		my $cv = AnyEvent->condvar;
		
		my $ac = $self->createConnection($cluster, $target);
		$self->connectOne($ac, sub {
			$cv->send;
		});
		$cv->recv;
	}

	$self->{log}->log("connected to all my KVDs for set >>$set<<");
}

sub createConnection {
	my ($self, $cluster, $target, $cb) = @_;

	weaken $self;

	my $serv = $cluster->[$target];
	my $ac = new Sisyphus::Connector;
	$ac->{host} = $serv->{ip};
	$ac->{port} = $serv->{port};
	$ac->{protocolName} = "Trivial";

	$ac->{state} = "disconnected";

	$self->{log}->log("ip $serv->{ip}, port $serv->{port}");

	# this gets called from our server. perhaps as a response to a get or set
	# request, perhaps for something else
	$ac->{app_callback} = sub {
		my $m = shift;
		$m = from_json($m);
		my $request_id = $m->{request_id};
		my $cb = $self->{data_callbacks}->{$request_id};
		unless ($cb) {
			$self->{log}->log("received message from server for unrecognized request id $request_id");
			return;
		}
		# $self->{log}->log("found, and calling, callback for request $request_id");
		delete $self->{data_callbacks}->{$request_id};
		$cb->($m);
	};

	$cluster->[$target]->{ac} = $ac;

	return $ac;
}

# asynchronously connect to one kvd
sub connectOne {
	my $self = shift;
	my $ac = shift;
	my $cb = shift;

	weaken $self;

	# on_error callback for connection phase only- do appropriate log, but also do callback
	$ac->{on_error} = sub {
		$self->{log}->log("detected error while attempting to open connection to server $ac->{host}:$ac->{port}, deferring connection");
		$ac->{state} = "disconnected";
		$cb->(undef);
	};


	$ac->connect( sub {
		my $c = shift;

		# error callbacks for "live" connection
		$ac->{server_closed} = sub {
			$ac->{state} = "disconnected";
			$self->{log}->log("my server disconnected");
		};
		$ac->{on_error} = sub {
			$ac->{state} = "disconnected";
			$self->{log}->log("detected error on server connection");
		};
		
		$ac->{state} = "connected";
		$cb->($c);
	});

}

sub get {
	my ($self, $key, $cb) = @_;

	weaken $self;

	# ok the callback for this - if we find a row, call the callback with it
	# if not, we do the whole thing again with the pending cluster

	$self->_get($self->{set}, $self->{cluster}, $key, sub {

		my $r = shift;

		if (defined($r->{data})) {
			$cb->($r);
		} else {
			if (($self->{cluster_state} eq "pending")
			or ($self->{cluster_state} eq "pending-write")) {
				$self->{log}->log("failed to find >>$key<< in first bucket, trying pending cluster map");
				$self->_get($self->{pending_set},
				            $self->{pending_cluster},
				            $key, $cb);
			
			} else {
				$self->{log}->log("really failed to find >>$key<<");
				$cb->(undef);
			}
		}

	});
				

}
	

sub _get {
	my ($self, $set, $cluster, $key, $cb) = @_;
	
	$self->{log}->log("getting >>$key<<");
	
	my $serv = $set->get_target($key);
	my $ac = $cluster->[$serv]->{ac};

	my $request_id = $self->get_request_id();
	$self->{data_callbacks}->{$request_id} = $cb;
	my $j = new JSON;
	my $jj = $j->encode({
		command => "get",
		key => $key,
		request_id => $request_id,
	});
	$self->send($ac, $jj);
}

# should properly be called "connectAndSend", tries to connect to the ac if it's not connected
sub send {
	my ($self, $ac, $j) = @_;

	$self->{log}->log("asked to send on connection, state $ac->{state}");

	unless($ac->{state} eq "connected") {
		$self->connectOne($ac, sub {
			$ac->send($j);
		});
	} else {
		$ac->send($j);
	}

}

sub rehash {
	my ($self, $cb) = @_;

	my $ac = AnyEvent->condvar;

	my $rehashers = scalar(@{$self->{cluster}});

	$self->{log}->log("i see $rehashers rehashers");

	foreach my $serv (@{$self->{cluster}}) {

		my $rac = $serv->{ac};
		my $request_id = $self->get_request_id();

		$self->{log}->log("asking server $serv to rehash");

		$self->{data_callbacks}->{$request_id} = sub {
			$rehashers = $rehashers - 1;

			$self->{log}->log("server $serv reported being done with rehash, $rehashers to go");

			if ($rehashers == 0) {	
				$self->{log}->log("i think i'm done with all my rehashing.");
				$cb->();
			}
		};

		my $j = to_json({
			command => "rehash",
			request_id => $request_id,
		});

		$self->send($rac, $j);
	}

}

# see readme. run code on remote servers to modify records in the db.
sub update {
	my ($self, $keys, $code, $cb) = @_;

	$self->{log}->log("in UPDATE!");

	# we're allowing arbitrary code be run on remote servers, upon some arbitrary 
	# and potentially larege number of keys
	# we don't want to recompile the code remotely on each invocation,
	# so we need some way of "sessioning" the request so we can say "compile this code",
	# now run it on all this stuff. now get rid of it.

	my $servs;
	my $total_keys_ever = scalar(@$keys);
	my $total_updates_done = 0;

	# gather up list of servers we're interested in, organize list of keys by server
	foreach my $key (@$keys) {
		
		$self->{log}->log("key $key!");
		# sort in to buckets...
		my $serv = $self->{set}->get_target($key);
		$servs->{$serv}->{ac} = $self->{cluster}->[$serv]->{ac};
		$servs->{$serv}->{compiled} = 0;
		push(@{$servs->{$serv}->{keys}}, $key);
	}
	
	# ask them to compile the the code...	
	$self->{log}->log(Dumper $servs);

	foreach my $serv (keys(%{$servs})) {

		my $evaluate_request_id = $self->get_request_id();

		$self->{data_callbacks}->{$evaluate_request_id} = sub {
			my $r = shift;
			my $remote_code_id = $r->{remote_code_id};

			$self->{log}->log("in evaluate callback, code $remote_code_id");

			# ohkay. this particular server has compiled the code, and given us
			# an id with which to refer to the remotely-compiled code
			# we ask it to run the code on each object in turn...

			my $this_server_updates_done = 0;
			my $this_server_keys = scalar(@{$servs->{$serv}->{keys}});

			# actually send all the update requests. the code is compiled, we just
			# send a ref to it. we do this serially instead of in a batch so the client
			# can control the rate of injection. not sure that's the right way, maybe
			# we should send the server a whole array of keys
			foreach my $k (@{$servs->{$serv}->{keys}}) {

				my $apply_request_id = $self->get_request_id();
				$self->{data_callbacks}->{$apply_request_id} = sub {
					# this gets called when the remote server is done updating
					# a single record

					$this_server_updates_done += 1;
					$total_updates_done += 1;
					if ($this_server_updates_done == $this_server_keys) {
						$self->{log}->log("this server is done with all its updates");
						# here's where you'd ask the server to remove its compiled code
					} else {
						$self->{log}->log("this server is done with $this_server_updates_done of $this_server_keys updates");
					}
					
					if ($total_updates_done == $total_keys_ever) {
						$self->{log}->log("i believe i'm done with all my updates across all servers");
						$cb->();
					} else {
						$self->{log}->log("i'm done with $total_updates_done of $total_keys_ever updates");
					}
				};

				my $o = to_json({
					command => "apply",
					code_id => $remote_code_id,
					request_id => $apply_request_id,
					key => $k,
				});
				$self->{log}->log("serv is $serv, ac is $servs->{$serv}->{ac}");
				$self->send($servs->{$serv}->{ac}, $o);
			}

		};

		# ok, actually send the compile request, which will set in to action all the 
		# things above.
		my $o = to_json({
			command => "evaluate",
			code => $code,
			request_id => $evaluate_request_id,
		});
	
		$self->send($servs->{$serv}->{ac}, $o);
	}
}

sub set {
	my ($self, $key, $val, $cb) = @_;
	$self->{log}->log("setting >>$key<<");

	# figure out what server and connection we should be using.
	my ($serv, $ac);	

	# if we're in pending-write state, we need to:
	# see if this object hashes to different db's in the "old" vs "new" clusters
	# if so, we want to send a "delete" to the old cluster
	# then send a set to the new cluster
	# a strong case could be made that this should be done in the server
	if ($self->{cluster_state} eq "pending-write") {
		my $serv = $self->{set}->get_target($key);
		my $pending_serv = $self->{pending_set}->get_target($key);

		if ($serv ne $pending_serv) {
			$self->{log}->log(">>$key<< hashes to different servers in the old cluster vs the new cluster. deleting from old cluster before the write to the new one.");

			my $dac = $self->{cluster}->[$serv]->{ac};

			# we're going to ignore the delete callback. hope it's OK!
			my $j = to_json({
				command => "delete",
				key => $key,
				});

			$self->send($dac, $j);

		} else {
			$self->{log}->log(">>$key<< hashes to same server in both new and old cluster maps ($serv vs $pending_serv)");
		}

		$serv = $self->{pending_set}->get_target($key);
		$ac = $self->{pending_cluster}->[$serv]->{ac};
	} else {
		$serv = $self->{set}->get_target($key);
		$ac = $self->{cluster}->[$serv]->{ac};
	}

	my $request_id = $self->get_request_id();
	$self->{data_callbacks}->{$request_id} = $cb;

	my $j = new JSON;
	$j->allow_blessed();
	$j->convert_blessed();
	my $o = $j->encode({
		command => "set",
		key => $key,
		data => $val,
		request_id => $request_id,
	});
	# print "json $o\n";	
	$self->send($ac, $o);
}

sub get_request_id {
	my $self = shift;
	$self->{request_id} += 1;

	return $self->{request_id};
}

1;
