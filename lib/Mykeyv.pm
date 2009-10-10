package Mykeyv;

# this is the keyval client lib. it connects to a mysql server
# and queries that DB for key/val data
#
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
use Digest::MD5 qw(md5);
use Storable;
use Data::Dumper;
use Set::ConsistentHash;
use String::CRC32;
use Sislog;
use AnyEvent;
use AnyEvent::Socket;

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
	my $cv = AnyEvent->condvar;
	$self->{pool}->connect(
		sub {
			# $self->{log}->log("connected to local mysql instance");
			$cv->send;
		}
	);
	$cv->recv; 

	my $self = bless($self, $class);
	$self->{pool}->{release_cb} = sub { $self->service_queryqueue };

	return $self;
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
				#$self->{log}->log("DONE in get callback");
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

# alert. will *replace* existing values.
sub set {
	my ($self, $key, $value, $cb) = @_;

	# try to get bucket contents
	my ($row, $got);
	$self->_getBucket($key, sub {
		$got = shift;
		$got = $got->[0];

		# this will either remain undef, or be set to the bucket
		# as it is in the DB.
		my $bucket;
		if (defined($got)) {
			if ($got eq "DONE") {
				# this is _getBucket telling us it returned all its data...
				if (defined($row->{$key})) {

					$self->{log}->log("replacing duplicate key >>$key<<");
					$bucket = $row;

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

			} else {
				# whereas this is _getBucket actually giving us a row
				$row = Storable::thaw($got);
			}
		} else {
			$self->{log}->log("unexpected, empty row for key >>$key<<");
		}

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


	my $md5key = md5($key);	
	$md5key =~ s/\'/\\\'/g;

	my $q = <<QQ;
SELECT
	TheValue
FROM
	$self->{table}	
WHERE
	Thekey = '$md5key'
QQ
	$self->{log}->log($q);
	push(@{$self->{queryqueue}}, sub {
		my $ac = $self->{pool}->claim();
		$ac->{connection}->{protocol}->query(
			q      => $q,
			cb     => sub {
				my $row = shift;
				if ($row eq "DONE") {
					$self->{log}->log("got DONE in callback");
					$self->{pool}->release($ac);
					$cb->(["DONE"]);
				} else {
					$self->{log}->log("got A ROW in callback");
					$cb->($row);
				}
			},
		);
	});

	$self->service_queryqueue();
} 

sub _setBucket {
	my ($self, $key, $value, $cb) = @_;

	my $md5key = md5($key);	
	$md5key =~ s/\'/\\\'/g;
	$value =~ s/\'/\\\'/g;

	my $q = <<QQ;
INSERT INTO
	$self->{table}
SET
	TheKey = '$md5key', TheValue = '$value'
ON DUPLICATE KEY UPDATE
	TheValue = '$value'
QQ

	push(@{$self->{queryqueue}}, sub {
		# $self->{log}->log("in queryqueue callback.");
		my $ac = $self->{pool}->claim();
		$ac->{connection}->{protocol}->query(
			q      => $q,
			cb     => sub {
				my $row = shift;
				$self->{pool}->release($ac);
				$cb->($row);
			},
		);
	});

	$self->service_queryqueue();
} 

1;
