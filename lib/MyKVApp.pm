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
use Scalar::Util qw/ weaken /;

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
	my ($ip, $port, $cid) = @_;
	
	# create an array to hold return values
	$self->{return_values}->{$cid} = [];
}

sub remote_closed {
    my ($self, $host, $port, $cid) = @_;

    print STDERR "$host closed connection!\n";
}

# called by framework
sub message {
	my ($self, $host, $port, $message, $cid) = @_;

	foreach my $m (@$message) {
		$m = from_json($m);
		$self->{log}->log("command $m->{command} rid $m->{request_id}");
		if ($m->{command} eq "set") {
			$self->do_set($cid, $m->{key}, $m->{data}, $m->{request_id});
		} elsif ($m->{command} eq "get") {
			$self->do_get($cid, $m->{key}, $m->{request_id});
		} elsif ($m->{command} eq "delete") {
			$self->do_delete($cid, $m->{key}, $m->{request_id});
		} elsif ($m->{command} eq "rehash") {
			$self->do_rehash($cid, $m->{request_id});
		} elsif ($m->{command} eq "evaluate") {
			$self->do_evaluate($cid, $m->{code}, $m->{request_id});
		} elsif ($m->{command} eq "apply") {
			$self->do_apply($cid, $m->{code_id}, $m->{key}, $m->{args}, $m->{request_id});
		} elsif ($m->{command} eq "list") {
			$self->do_list($cid, $m->{request_id});
		} else {
			$self->{log}->log("got unknown command $m->{command}");
		}
	}

}


sub do_apply {
	my ($self, $cid, $code_id, $key, $args, $client_rid) = @_;

	weaken $self;

	# ok rad.
	$self->{log}->log("do apply, code id $code_id key $key");

	$self->{kv}->apply($code_id, $key, $args, sub {
		my $result = shift;
		# XXX actually check return values, pls

		my $r = to_json({
			command => "apply_ok",
			request_id => $client_rid,
		});

		push (@{$self->{return_values}->{$cid}}, $r);
		$self->{client_callback}->([$cid]);		
	});
}

sub do_evaluate {
	my ($self, $cid, $code_scalar, $client_rid) = @_;

	my $runme = "my \$s = sub { $code_scalar }; return \$s;";
	my $sub;
	eval {
		$sub = eval $runme;
	};
	# check $@ and $! here. deal if needed.

	# no reason this should block, no need to do it async'ly
	my $code_id = $self->{kv}->set_evaluated($sub);

	$self->{log}->log("evaluated code id $code_id for rid $client_rid");

	# ok even though nothing has blocked, we return to our client using a callback
	# in the future, we might want to do eval's in their own processes, to guard against
	# long-running failures or something

	my $r = to_json({
		command => "evaluate_ok",
		request_id => $client_rid,
		remote_code_id => $code_id,
	});

	push (@{$self->{return_values}->{$cid}}, $r);
	$self->{client_callback}->([$cid]);		
}

sub do_set {
	my ($self, $cid, $key, $val, $client_rid) = @_;

	weaken $self;

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
			push (@{$self->{return_values}->{$cid}}, $r);
			$self->{client_callback}->([$cid]);		
		}
	);
}

sub do_get {
	my ($self, $cid, $key, $client_rid) = @_;

	weaken $self;

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
			push (@{$self->{return_values}->{$cid}}, $r);
			$self->{client_callback}->([$cid]);		
		}
	);
}

sub do_list {
	my ($self, $cid, $client_rid) = @_;

	weaken $self;

	my $rid = $self->getRID();
	$self->{log}->log("doing list id $client_rid");
	$self->{kv}->list(sub {
		my $keys = shift;
		$self->{log}->log("do_list callback rid $client_rid");
		my $r = to_json({
			command => "list_ok",
			data => $keys,
			request_id => $client_rid,
		});
		push (@{$self->{return_values}->{$cid}}, $r);
		$self->{client_callback}->([$cid]);
	});
}

sub do_rehash {
	my ($self, $cid, $client_rid) = @_;

	weaken $self;

	my $rid = $self->getRID();

	$self->{kv}->rehash(sub {
			my $v = shift;
			$self->{log}->log("got notified of rehash being done");
			my $r = to_json({
				command => "rehash_ok",	
				request_id => $client_rid,
			});
			push (@{$self->{return_values}->{$cid}}, $r);
			$self->{client_callback}->([$cid]);		
		}
	);
}


sub do_delete {
	my ($self, $cid, $key, $client_rid) = @_;

	weaken $self;

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
			push (@{$self->{return_values}->{$cid}}, $r);
			$self->{client_callback}->([$cid]);		
		}
	);
}

sub get_data {
	my ($self, $cid) = @_;
	my $v = pop(@{$self->{return_values}->{$cid}});
	return $v;
}

1;
