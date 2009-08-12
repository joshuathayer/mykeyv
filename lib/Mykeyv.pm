package Mykeyv;

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
	$self->{qtable} = {};

	my $cv = AnyEvent->condvar;
	$self->{ac}->connectAsync(
		sub {
			print STDERR "connected to local mysql instance\n";
			$cv->send;
		}
	);
	$cv->recv; 
	return bless($self, $class);
}

sub getQID {
	my $self = shift;
	$self->{qc} += 1;
	return $self->{qc};
}

sub get {
	my ($self, $key, $cb) = @_;

	my $qid = $self->getQID();
	$self->{qtable}->{$qid}->{value} = undef;

	# try to get bucket contents
	my $row;

	$self->_getBucket($key, $qid, sub {
		my $rqid = shift;
		$row = shift;

		$row = $row->[0];

		if (defined($row)) {
			#print STDERR "got bucket with content\n";
			$row = Storable::thaw($row);
			if (defined($row->{$key})) {
				#print STDERR "got bucket with this key!\n";
				$self->{qtable}->{$qid}->{value} = $row->{$key};
			}
		} else {
			#print STDERR "got NULL ROW- end of set, or empty set\n";
			$cb->( $self->{qtable}->{$qid}->{value} );
			delete $self->{qtable}->{$qid};
		}
	});
}

# alert. will *replace* existing values.
sub set {
	my ($self, $key, $value, $cb) = @_;

	# this is entry in the query table for this entire 'set' query
	my $qid = $self->getQID();
	$self->{qtable}->{$qid}->{cb} = $cb;
	$self->{qtable}->{$qid}->{key} = $key;
	$self->{qtable}->{$qid}->{bucket} = {};

	# try to get bucket contents
	my $row;
	$self->_getBucket($key, $qid, sub {

		$row = shift;
		$row = $row->[0];

		if (defined($row)) {
			#print STDERR "got bucket with content\n";
			$row = Storable::thaw($row);
			if (defined($row->{$key})) {
				print STDERR "replacing duplicate key >>$key<<\n";
			}
			$self->{qtable}->{$qid}->{bucket} = $row;
			$self->{qtable}->{$qid}->{value};
		} else {
			#print STDERR "got NULL ROW- end of set, or empty set\n";
			$self->{qtable}->{$qid}->{bucket}->{$key} = $value;
			#print Dumper  $self->{qtable}->{$qid}->{bucket};
			$row = Storable::freeze($self->{qtable}->{$qid}->{bucket});

			$self->_setBucket($key, $row, sub {
				# if we're back here, we've set the proper row in the db
				#print STDERR "looks like we updated the table\n";
				$cb->();
				delete $self->{qtable}->{$qid};
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
	my ($self, $key, $qid, $cb) = @_;
	
	
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
		qid    => $qid,
		cb     => sub {
			my $row = shift;
			$cb->($qid, $row);
		},
	);

} 

sub _setBucket {
	my ($self, $key, $value, $qid, $cb) = @_;
	
	
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
		qid    => $qid,
		cb     => sub {
			my $row = shift;
			#print "---> received ID $row->[0]\n";
			$cb->($qid, $row);
		},
	);

} 

1;
