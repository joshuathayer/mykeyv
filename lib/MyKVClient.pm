package MyKVClient;

# MyKV client library.

use lib ("/Users/joshua/projects/sisyphus/lib/");

use AnyEvent::Strict;
use Sisyphus::Connector;
use Sislog;
use JSON;
use Data::Dumper;

use Set::ConsistentHash;
use String::CRC32;

sub new {
	my $class = shift;
	my $in = shift;

	my $self = {
		set => Set::ConsistentHash->new,
		cluster => $in->{cluster},
		prevset => undef,
		prevcluster => undef,
		log => Sislog->new({use_syslog => 1, facility=>"MyKVClient"}),
		request_id => 0,
		data_callbacks => {},
	};

	$self->{log}->open();

	# set is a ConsistentHash object.
	$self->{set}->set_hash_func(\&crc32);

	# cluster is an array of hashes.  each hash represents a kvd server.
	# we want to add each element of the cluster array into the
	# ConsistentHash, with even weights.
	my $i = 0; my $targets;
	foreach my $t (@{$self->{cluster}}) {
		$targets->{$i} = 1;
		$i += 1;
	}
	$self->{set}->modify_targets( %$targets );

	# bless the object now, so we can call methods on it
	bless $self, $class;

	# this will need real thought regarding errors and disconnections	
	$self->makeConnections();	
	
	return $self;
}

# connect to every kvd server. blocks!
sub makeConnections {
	my $self = shift;

	foreach my $target ($self->{set}->targets()) {
		my $cv = AnyEvent->condvar;
		$self->connectOne($target, sub {
			$cv->send;
		});
		$cv->recv;
	}
	$self->{log}->log("connected to all my KVDs");
}

# asynchronously connect to one kvd
sub connectOne {
	my ($self, $target, $cb) = @_;

	my $serv = $self->{cluster}->[$target];
	my $ac = new Sisyphus::Connector;
	$ac->{host} = $serv->{ip};
	$ac->{port} = $serv->{port};
	$ac->{protocolName} = "Trivial";

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

	# placeholder connection closed/error callbacks
	$ac->{server_closed} = sub { $self->{log}->send("my server disconnected",time); };
	$ac->{on_error} = sub { $self->{log}->log("detected error on server connection",time); };

	$self->{cluster}->[$target]->{ac} = $ac;

	# this sub doesn't actually care about the connection,
	# we just pass the caller's callback
	$ac->connect($cb);
}

sub get {
	my ($self, $key, $cb) = @_;
	
	$self->{log}->log("getting >>$key<<");
	
	my $serv = $self->{set}->get_target($key);
	my $ac = $self->{cluster}->[$serv]->{ac};
	
	my $request_id = $self->get_request_id();
	$self->{data_callbacks}->{$request_id} = $cb;

	my $j = to_json({
		command => "get",
		key => $key,
		request_id => $request_id,
	});

	$ac->send($j);
}

sub set {
	my ($self, $key, $val, $cb) = @_;
	$self->{log}->log("setting >>$key<<");
	
	my $serv = $self->{set}->get_target($key);
	my $ac = $self->{cluster}->[$serv]->{ac};

	my $request_id = $self->get_request_id();
	# $self->{log}->log("setting request callback for request $request_id in set");
	$self->{data_callbacks}->{$request_id} = $cb;

	my $o = to_json({
		command => "set",
		key => $key,
		data => $val,
		request_id => $request_id,
	});
	
	$ac->send($o);
}

sub get_request_id {
	my $self = shift;
	$self->{request_id} += 1;

	return $self->{request_id};
}

1;
