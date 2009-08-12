package MyKVApp;

# this is the keyval daemon app
# uses the Keyv client lib

use lib ("/home/joshua/projects/sisyphus/lib");
use lib ("/home/joshua/projects/keyv/lib");

use strict;
use Mykeyv;
use Data::Dumper;
use JSON;

# TODO
# alarms to clear return_values

sub new {
	my $class = shift;
	my $self = {
		return_values => {},
	};

	$self->{rid} = 0;	

	my $kv = Mykeyv->new({
		'host' => "127.0.0.1",
		'port' => 3306,
		'user' => 'keyvalue',
		'pw' => 'roses',
		'db' => 'keyvalue',
	});

	$self->{kv} = $kv;

	return(bless($self, $class));
}

# create a new request ID
sub getRID {
	my $self = shift;
	$self->{rid} += 1;
	return $self->{rid};
}

sub new_connection {
	my $self = shift;
	my ($ip, $port, $fh) = @_;
	
	# create an array to hold return values
	$self->{return_values}->{$fh} = [];
}

sub remote_closed {
    my ($self, $host, $port, $fh) = @_;

    print STDERR "$host closed connection!\n";
}

# called by framework
sub message {
	my ($self, $host, $port, $message, $fh) = @_;

	#print "received a message from host $host port $port:\n";
	#print Dumper $message;
	
	foreach my $m (@$message) {
		$m = from_json($m);
		if ($m->{command} eq "set") {
			$self->do_set($fh, $m->{key}, $m->{data}, $m->{request_id});
		} elsif ($m->{command} eq "get") {
			$self->do_get($fh, $m->{key}, $m->{request_id});
		}
	}

}

sub do_set {
	my ($self, $fh, $key, $val, $client_rid) = @_;
	my $rid = $self->getRID();

	print STDERR "set >>$key<<\n";

	$self->{kv}->set($key, $val, sub {
			my $v = shift;
			my $r = to_json({
				command => "set_ok",	
				key => $key,
				data => $v,
				request_id => $client_rid,
			});
			push (@{$self->{return_values}->{$fh}}, $r);
			$self->{client_callback}->([$fh]);		
		}
	);
}

sub do_get {
	my ($self, $fh, $key, $client_rid) = @_;
	my $rid = $self->getRID();

	print STDERR "get >>$key<<\n";

	$self->{kv}->get($key, sub {
			my $v = shift;
			my $r = to_json({
				command => "get_ok",	
				key => $key,
				data => $v,
				request_id => $client_rid,
			});
			push (@{$self->{return_values}->{$fh}}, $r);
			$self->{client_callback}->([$fh]);		
		}
	);

}

sub get_data {
	my ($self, $fh) = @_;
	my $v = pop(@{$self->{return_values}->{$fh}});
	return $v;
}


1;
