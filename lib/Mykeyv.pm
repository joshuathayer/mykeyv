package Mykeyv;

# this is the keyval client lib. it connects to a mysql server
# and queries that DB for key/val data
#
# the main points of entry are get(), set(), getSync(), and setSync()

use strict;
use Sisyphus::Connector;
use Sisyphus::Proto::Factory;
use Digest::MD5 qw(md5);
use Storable;
use Data::Dumper;

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

	$self->{ac} = new Sisyphus::Connector;
	$self->{ac}->{host} = $in->{host};
	$self->{ac}->{port} = $in->{port};

	$self->{ac}->{protocolName} = "Mysql";
	$self->{ac}->{protocolArgs} = {
		user => $in->{user},
		pw => $in->{pw},
		db => $in->{db},
		err => sub {
			my $err = shift;
			print STDERR "An error occured while querying MySQL: $err\n";
		},
	}; 

	my $cv = AnyEvent->condvar;
	$self->{ac}->connect(
		sub {
			print STDERR "connected to local mysql instance\n";
			$cv->send;
		}
	);
	$cv->recv; 
	return bless($self, $class);
}

sub get {
	my ($self, $key, $cb) = @_;

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
}

# alert. will *replace* existing values.
sub set {
	my ($self, $key, $value, $cb) = @_;

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

	$self->{ac}->{protocol}->query(
		q      => $q,
		cb     => sub {
			my $row = shift;
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

	$self->{ac}->{protocol}->query(
		q      => $q,
		#qid    => $qid,
		cb     => sub {
			my $row = shift;
			#print "---> received ID $row->[0]\n";
			$cb->($row);
		},
	);

} 

1;
