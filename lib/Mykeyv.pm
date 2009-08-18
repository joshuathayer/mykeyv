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


# constructor.
# note that THIS WILL BLOCK CALLING CODE
# it would be trivially possible to create a nonblocking constructor
# or connect method, but i don't see the point?
sub new {
	my $class = shift;
	my $in = shift;

	my $self = { };

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
			print STDERR "An error occured while querying MySQL: $err\n";
		},
	}; 

	my $cv = AnyEvent->condvar;
	$self->{pool}->connect(
		sub {
			print STDERR "connected to local mysql instance\n";
			$cv->send;
		}
	);

	$cv->recv; 

	my $self = bless($self, $class);
	$self->{pool}->{release_cb} = sub { $self->service_queryqueue-() };

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


	push(@{$self->{queryqueue}}, sub {

		# try to get bucket contents
		my $row;
		my $val;

		$self->_getBucket($key, sub {
			$row = shift;

			$row = $row->[0];
	
			if (defined($row)) {
				#print STDERR "got bucket with content\n";
				$row = Storable::thaw($row);
				if (defined($row->{$key})) {
					# print STDERR "got bucket with this key!\n";
					$val = $row->{$key};
				}
			} else {
				# print STDERR "got NULL ROW- end of set, or empty set\n";
				$cb->($val);
			}
		});
	});

	$self->service_queryqueue();
}

# alert. will *replace* existing values.
sub set {
	my ($self, $key, $value, $cb) = @_;

	push(@{$self->{queryqueue}}, sub {
		my $bucket = {};
		# try to get bucket contents
		my $row;
		$self->_getBucket($key, sub {
			$row = shift;
			$row = $row->[0];

			if (defined($row)) {
				#print STDERR "got bucket with content\n";
				$row = Storable::thaw($row);
				if (defined($row->{$key})) {
					print STDERR "replacing duplicate key >>$key<<\n";
				}
				$bucket = $row;
			} else {
				#print STDERR "got NULL ROW- end of set, or empty set\n";
				$bucket->{$key} = $value;
				$row = Storable::freeze($bucket);

				$self->_setBucket($key, $row, sub {
					# if we're back here, we've set the proper row in the db
					#print STDERR "looks like we updated the table\n";
					$cb->();
				});
			}
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


	my $md5key = md5($key);	
	$md5key =~ s/\'/\\\'/g;

	my $q = <<QQ;
SELECT
	TheValue
FROM
	Keyvalue
WHERE
	Thekey = '$md5key'
QQ

	my $ac = $self->{pool}->claim();
	$ac->{connection}->{protocol}->query(
		q      => $q,
		cb     => sub {
			my $row = shift;
			unless ($row) {
				# if we get an empty row, we're done.
				$self->{pool}->release($ac);
			}
			$cb->($row);
		},
	);

} 

sub _setBucket {
	my ($self, $key, $value, $cb) = @_;

	my $md5key = md5($key);	
	$md5key =~ s/\'/\\\'/g;
	$value =~ s/\'/\\\'/g;

	my $q = <<QQ;
INSERT INTO
	Keyvalue
SET
	TheKey = '$md5key', TheValue = '$value'
ON DUPLICATE KEY UPDATE
	TheValue = '$value'
QQ

	my $ac = $self->{pool}->claim();
	$ac->{connection}->{protocol}->query(
		q      => $q,
		#qid    => $qid,
		cb     => sub {
			my $row = shift;
			$self->{pool}->release($ac);
			#print "---> received ID $row->[0]\n";
			$cb->($row);
		},
	);

} 

1;
