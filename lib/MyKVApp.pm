package MyKVApp;

# this is the keyval daemon App
# uses the Keyv lib to do the actual talking to Mysql
# is used by kvd, and is an App in the Sisyphus framework sense

use strict;
use Mykeyv::Mykeyv;
use Data::Dumper;
use JSON;
use Set::ConsistentHash;
use String::CRC32;

# TODO
# alarms to clear return_values

sub new {
	my $class = shift;

	my ($dbip, $dbport, $dbuser, $dbpw, $dbdb, $dbtable, $log,
	    $cluster, $pending_cluster, $cluster_state) = @_;

	my $self = {
		return_values => {},
	};

	$self->{rid} = 0;
	$self->{log} = $log;

	my $kv = Mykeyv::Mykeyv->new({
		'host' => $dbip, # "127.0.0.1",
		'port' => $dbport, # 3306,
		'user' => $dbuser, # 'KeyValue',
		'pw' => $dbpw, # 'KeyValuePass',
		'db' => $dbdb, # 'KeyValue',
		'table' => $dbtable, 
		'log' => $log,
		'cluster' => $cluster,
		'pending_cluster' => $pending_cluster,
		'cluster_state' => $cluster_state,
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
		$self->{log}->log("command $m->{command}");
		if ($m->{command} eq "set") {
			$self->do_set($fh, $m->{key}, $m->{data}, $m->{request_id});
		} elsif ($m->{command} eq "get") {
			$self->do_get($fh, $m->{key}, $m->{request_id});
		} elsif ($m->{command} eq "delete") {
			$self->do_delete($fh, $m->{key}, $m->{request_id});
		} elsif ($m->{command} eq "rehash") {
			$self->do_rehash($fh, $m->{request_id});
		} elsif ($m->{command} eq "evaluate") {
			$self->do_evaluate($fh, $m->{code}, $m->{request_id});
		} elsif ($m->{command} eq "apply") {
			$self->do_apply($fh, $m->{code_id}, $m->{key}, $m->{args}, $m->{request_id});
		} else {
			$self->{log}->log("got unknown command $m->{command}");
		}
	}

}


sub do_apply {
	my ($self, $fh, $code_id, $key, $args, $client_rid) = @_;

	# ok rad.
	$self->{log}->log("do apply, code id $code_id key $key");

	$self->{kv}->apply($code_id, $key, $args, sub {
		my $result = shift;
		# XXX actually check return values, pls

		my $r = to_json({
			command => "apply_ok",
			request_id => $client_rid,
		});

		push (@{$self->{return_values}->{$fh}}, $r);
		$self->{client_callback}->([$fh]);		
	});
}

sub do_evaluate {
	my ($self, $fh, $code_scalar, $client_rid) = @_;

	my $runme = "my \$s = sub { $code_scalar }; return \$s;";
	my $sub;
	eval {
		$sub = eval $runme;
	};
	# check $@ and $! here. deal if needed.

	# no reason this should block, no need to do it async'ly
	my $code_id = $self->{kv}->set_evaluated($sub);

	$self->{log}->log("code id $code_id");

	# ok even though nothing has blocked, we return to our client using a callback
	# in the future, we might want to do eval's in their own processes, to guard against
	# long-running failures or something

	my $r = to_json({
		command => "evaluate_ok",
		request_id => $client_rid,
		remote_code_id => $code_id,
	});

	push (@{$self->{return_values}->{$fh}}, $r);
	$self->{client_callback}->([$fh]);		
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

sub do_rehash {
	my ($self, $fh, $client_rid) = @_;
	my $rid = $self->getRID();

	$self->{kv}->rehash(sub {
			my $v = shift;
			$self->{log}->log("got notified of rehash being done");
			my $r = to_json({
				command => "rehash_ok",	
				request_id => $client_rid,
			});
			push (@{$self->{return_values}->{$fh}}, $r);
			$self->{client_callback}->([$fh]);		
		}
	);
}


sub do_delete {
	my ($self, $fh, $key, $client_rid) = @_;
	my $rid = $self->getRID();

	$self->{log}->log("delete >>$key<< id $client_rid");

	$self->{kv}->delete($key, sub {
			my $v = shift;
			my $r = to_json({
				command => "delete_ok",	
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
