package MyKVApp;

# this is the keyval daemon App
# uses the Keyv lib to do the actual talking to Mysql
# is used by kvd, and is an App in the Sisyphus framework sense

use lib ("/home/joshua/projects/sisyphus/lib");
use lib ("/home/joshua/projects/keyv/lib");

use strict;
use Mykeyv;
use Data::Dumper;
use JSON;
use Set::ConsistentHash;
use String::CRC32;

# TODO
# alarms to clear return_values

sub new {
	my $class = shift;
	my ($dbip, $dbport, $dbuser, $dbpw, $dbdb, $dbtable, $log) = @_;
	my $self = {
		return_values => {},
	};

	$self->{rid} = 0;
	$self->{log} = $log;

	my $kv = Mykeyv->new({
		'host' => $dbip, # "127.0.0.1",
		'port' => $dbport, # 3306,
		'user' => $dbuser, # 'keyvalue',
		'pw' => $dbpw, # 'roses',
		'db' => $dbdb, # 'keyvalue',
		'table' => $dbtable, 
		'log' => $log,
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

	$self->{log}->log("set >>$key<<");

	$self->{kv}->set($key, $val, sub {
			my $v = shift;
			print "in do_set callback.\n";
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

	$self->{log}->log("get >>$key<< id $client_rid");

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
