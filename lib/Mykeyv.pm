package Mykeyv;

# the main points of entry are get(), set(), getSync(), and setSync()
#
# CLUSTER INFORMATION
# For normal "persistant-connection" operation, this module doesn't need
# to know about how keys are hashed- the client keeps that state, and 
# will send right queries to the right places. However, once "one-off"
# queries are implemented (by scripts that aren't long-running enough to
# figure out the state of the DHT on their own), the module might be 
# called upon to forward queries to the right places. Meaning it will
# want to have an idea of what the DHT looks like. Also, for rehashing
# after adding a bucket, etc, this module might want to know wtf.
# Of course, we could simplify by saying that all clients must send the 
# queries to the right place on their own, and that rehashing must be 
# taken care of by a special purpose client, and that would simplify
# this module considerably at the expense of added complexity and perhaps
# decreased efficiency elsewhere.

use strict;
use Sisyphus::Connector;
use Sisyphus::ConnectionPool;
use Sisyphus::Proto::Factory;
use Sisyphus::Proto::Mysql;
use Digest::MD5 qw(md5_base64);
use Storable;
use Data::Dumper;
use Set::ConsistentHash;
use String::CRC32;
use Sislog;
use AnyEvent;
use AnyEvent::Socket;
use Data::HexDump;
use MyKVClient;

# constructor.
# note that THIS WILL BLOCK CALLING CODE
# it would be trivially possible to create a nonblocking constructor
# or connect method, but i don't see the point?
sub new {
	my $class = shift;
	my $in = shift;

	my $self = { };

	# logging
	$self->{log} = Sislog->new({use_syslog=>1, facility=>"Mykeyv"});
	$self->{log}->open();

	#$self->{log}->log("instantiating Mykeyv object");

	# query counter. internal index of queries
	$self->{qc} = 0;

	# query queue. 
	$self->{queryqueue} = [];

	# set up pool of Mysql connections, per 
	$self->{pool} = new Sisyphus::ConnectionPool;
	$self->{pool}->{host} = $in->{host};
	$self->{pool}->{port} = $in->{port};
	$self->{pool}->{connections_to_make} = 10;
	$self->{pool}->{protocolName} = "Mysql";
	$self->{pool}->{protocolArgs} = {
		user => $in->{user},
		pw => $in->{pw},
		db => $in->{db},
		err => sub {
			my $err = shift;
			$self->{log}->log("An error occured while querying MySQL: $err");
		},
	}; 

	# table we should use. for development, we might want multiple
	# tables per mysql server
	$self->{table} = $in->{table};

	# cluster config information
	$self->{cluster} = $in->{cluster};
	$self->{pending_cluster} = $in->{pending_cluster};
	$self->{cluster_state} = $in->{cluster_state};


	my $cv = AnyEvent->condvar;
	$self->{pool}->connect(
		sub {
			$self->{log}->log("connected to local mysql instance");
			$cv->send;
		}
	);
	$cv->recv; 

	# we're also a client of our other servers...
	$self->{kvc} = MyKVClient->new({
		cluster => $self->{cluster},
		pending_cluster => $self->{pending_cluster},
		cluster_state => $self->{cluster_state},
	});

	my $self = bless($self, $class);

	# just like in the client object, we maintain a Set object, for the consistent hashing
	# this needs to be maintained for rehashing, and for potetial future things.
	$self->prep_set("set", $self->{cluster});
	if (($self->{cluster_state} eq "pending") or ($self->{cluster_state} eq "pending-write")) {
		$self->prep_set("pending_set", $self->{pending_cluster});
	}	

	$self->{pool}->{release_cb} = sub { $self->service_queryqueue };

	return $self;
}

# this is just as in client code. we need to maintain this struct for rehashing.
sub prep_set {
	my ($self, $set, $cluster) = @_;

	$self->{log}->log("prepping $set, which has " . scalar(@$cluster) . " members");

	$self->{ $set } = Set::ConsistentHash->new;
	$self->{ $set }->set_hash_func(\&crc32);

	my $i = 0; my $targets;
	foreach my $t (@{$cluster}) {
		$targets->{$i} = 1;
		$i += 1;
	}
	$self->{log}->log("prepped $i members");

	$self->{ $set }->modify_targets( %$targets );	
}

sub service_queryqueue {
	my $self = shift;

	if ($self->{pool}->claimable()) {
		if (scalar(@{$self->{queryqueue}})) {
			my $sub = pop(@{$self->{queryqueue}});
			$sub->();
		}
	}

}

sub get {
	my ($self, $key, $cb) = @_;

	# try to get bucket contents
	my $row;
	my $val;
	my $res;

	$self->_getBucket($key, sub {
		$row = shift;

		$row = $row->[0];
	
		if (defined($row)) {
			if ($row eq "DONE") {
				# ok this result is done.
				$self->{log}->log("DONE in get callback");
				$cb->($val);
			} else {
				# whereas this is real data
				$row = Storable::thaw($row);
				if (defined($row->{$key})) {
					#$self->{log}->log("get success");
					$val = $row->{$key};
				}
			}
		} else {
			# jt not sure in what instance we reach here.
			$self->{log}->log("get failure? _getBucket called its callback with undef");
			$cb->($val);
		}
	});

	$self->service_queryqueue();
}

# used by delete() and set(), we look for a bucket (row) for a particular key. 
# if it exists, we return it as a hash. if not we return an empty hash.
sub _getBucketIfExists {
	my ($self, $key, $cb) = @_;

	my $bucket = {};
	# try to get bucket contents, or return an empty hash if there's nothing in the db
	$self->_getBucket($key, sub {
		my $got = shift;

		$got = $got->[0];

		if (defined($got)) {
			if ($got eq "DONE") {
				# this is _getBucket telling us it returned all its data...
				if (scalar(keys(%$bucket))) { 
					$self->{log}->log("found extant bucket for >>$key<<");
				} else {
					$self->{log}->log("no extant bucket for >>$key<<");
				}

				$cb->($bucket);	
			} else {
				# whereas this is _getBucket actually giving us a row
				$self->{log}->log("found existing bucket for key >>$key<<");
				$bucket = Storable::thaw($got);
			}
		} else {
			$self->{log}->log("unexpected, empty row for key >>$key<<");
			$cb->($bucket);
		}

	});

	$self->service_queryqueue();
};

sub delete {
	my ($self, $key, $cb) = @_;
	
	$self->{log}->log("delete >>$key<<");

	# try to get bucket contents
	my ($row, $got);
	$self->_getBucketIfExists($key, sub {
		my $bucket = shift;

		if (defined($bucket->{$key})) {

			$self->{log}->log(">>$key<< exists, ready to delete it");
			delete $bucket->{$key};
			$row = Storable::freeze($bucket);
		
			$self->_setBucket($key, $row, sub {
				# if we're back here, we've set the proper row in the db
				#$self->{log}->log("in set callback");
				$cb->();
			});
		} else {
			$self->{log}->log(">>$key<< doesn't exist, nothing to delete");
		}
	});	

	$self->service_queryqueue();
};

# alert. will *replace* existing values.
sub set {
	my ($self, $key, $value, $cb) = @_;

	# try to get bucket contents
	my ($row, $got);
	$self->_getBucketIfExists($key, sub {
		my $bucket = shift;

		if (defined($bucket->{$key})) {

			$self->{log}->log("replacing duplicate key >>$key<<");

		} else {

			$self->{log}->log("this looks like a new key >>$key<<");

		}	
		$bucket->{$key} = $value;
		$row = Storable::freeze($bucket);
		
		$self->_setBucket($key, $row, sub {
			# if we're back here, we've set the proper row in the db
			#$self->{log}->log("in set callback");
			$cb->();
		});
	});	

	$self->service_queryqueue();
};

sub getSync {
	my ($self, $key) = @_;
	my $v;

	my $cv = AnyEvent->condvar;
	$self->get(
	    $key, sub { $v = shift; $cv->send(); }
	);
	$cv->recv;

	return $v;

}

sub setSync {
	my ($self, $key, $val) = @_;
	my $v;

	my $cv = AnyEvent->condvar;
	$self->set(
	    $key, $val, sub { $v = shift; $cv->send(); }
	);
	$cv->recv;

	return $v;
}

sub _getBucket {
	my ($self, $key, $cb) = @_;

	my $md5key = md5_base64($key);

	my $q = <<QQ;
SELECT
	TheValue
FROM
	$self->{table}	
WHERE
	Thekey = '$md5key'
QQ
	push(@{$self->{queryqueue}}, sub {
		$self->{pool}->claim( sub {
			my $ac = shift;

			$ac->{protocol}->query(
				q      => $q,
				cb     => sub {
					my $row = shift;
					if ($row->[0] eq "DONE") {
						#$self->{log}->log("got DONE in callback");
						$self->{pool}->release($ac);
						$cb->(["DONE"]);
					} else {
						#$self->{log}->log("got A ROW in callback");
						$cb->($row);
					}
				},
			);
		});
	});

	$self->service_queryqueue();
} 

sub _setBucket {
	my ($self, $key, $value, $cb) = @_;

	my $md5key = md5_base64($key);
	$value = Sisyphus::Proto::Mysql::esc($value);

	my $q = <<QQ;
INSERT INTO
	$self->{table}
SET
	TheKey = '$md5key', TheValue = '$value'
ON DUPLICATE KEY UPDATE
	TheValue = '$value'
QQ
	#$self->{log}->log("in _setBucket");
	push(@{$self->{queryqueue}}, sub {
		#$self->{log}->log("in queryqueue callback.");
		$self->{pool}->claim(sub {
			my $ac = shift;
			$ac->{protocol}->query(
				q      => $q,
				cb     => sub {
					my $row = shift;
					$self->{pool}->release($ac);
					$cb->($row);
				},
			);
		});
	});

	$self->service_queryqueue();
} 

sub rehash {
	my ($self, $cb) = @_;
	
	# select every row in our table!
	my $q = <<QQ;
SELECT
	TheValue
FROM
	$self->{table}
QQ
	my $pending = 0;
	my $seenDone = 0;

	push(@{$self->{queryqueue}}, sub {
		$self->{pool}->claim(sub {
			my $ac = shift;

			$self->{log}->log("within claim callback. going to query.");

			$ac->{protocol}->query(
				q      => $q,
				cb     => sub {
					my $row = shift;
					$row = $row->[0];

					if ($row eq "DONE" ) {
						$seenDone = 1;
					} else {
						$pending += 1;
						$self->{log}->log("got a row in rehash.");
	
						my $bucket = Storable::thaw($row);
	
						$self->_rehashBucket($bucket, sub {
							$self->{pool}->release($ac);
							$pending -= 1;
						});
					}
					if ($seenDone and ($pending == 0)) {
						$self->{log}->log("got a DONE in rehash and pending==0, calling callback.");
						$cb->();
					}
				},
			);
		});	
	});

	$self->service_queryqueue();
}

sub _rehashBucket {
	my ($self, $bucket, $cb) = @_;

	foreach my $key (keys %$bucket) {
		$self->{log}->log("i'd rehash >>$key<<");

		# figure out what serv this should be on according to new cluster
		my $serv = $self->{set}->get_target($key);
		my $pending_serv = $self->{pending_set}->get_target($key);

		if ($serv ne $pending_serv) {
			$self->{log}->log(">>$key<< hashes to NEW server $serv vs $pending_serv");
			$self->get($key, sub {
				my $v = shift;
				$self->{log}->log("going to try calling >>set $key<< on my client");

				$self->{kvc}->set($key, $v, sub {
					$self->{log}->log("OK! migrated record.");
					$cb->();
				});
			});
		} else {
			$self->{log}->log(">>$key<< hashes to SAME server $serv vs $pending_serv");
			$cb->();
		}	
	}

}

1;
