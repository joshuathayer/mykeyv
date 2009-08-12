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

sub new_connection {
	my $self = shift;
	my @rest = @_;
}

sub remote_closed {
    my ($self, $host, $port, $fh) = @_;

    print STDERR "$host closed connection!\n";
}

sub message {
	my ($self, $host, $port, $message, $fh) = @_;

	#print "received a message from host $host port $port:\n";
	#print Dumper $message;
	#
	foreach my $m (@$message) {
		$m = from_json($m);
		if ($m->{command} eq "set") {
			$self->do_set($fh, $m->{key}, $m->{data});
		} elsif ($m->{command} eq "get") {
			$self->do_get($fh, $m->{key});
		}
	}

	# we return this filehandle- this will indicate to Sisyphus that we have
	# something to send back to the client. in this example case, we'll just
	# return a simple message

	#$self->{client_callback}->([$fh]);
}

sub do_set {
	my ($self, $fh, $key, $val) = @_;

	print STDERR "set >>$key<<\n";

	$self->{kv}->set($key, $val, sub {
			my $v = shift;
			my $r = to_json({
				command => "set_ok",	
				key => $key,
				data => $v,
			});
			$self->{return_values}->{$fh} = $r;
			$self->{client_callback}->([$fh]);		
		}
	);
}

sub do_get {
	my ($self, $fh, $key) = @_;

	print STDERR "get >>$key<<\n";

	$self->{kv}->get($key, sub {
			my $v = shift;
			my $r = to_json({
				command => "get_ok",	
				key => $key,
				data => $v,
			});
			$self->{return_values}->{$fh} = $r;
			$self->{client_callback}->([$fh]);		
		}
	);

}

sub get_data {
	my ($self, $fh) = @_;
	my $v = $self->{return_values}->{$fh};
	return $v;
}


1;
