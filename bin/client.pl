# example mykeyv client using sisyphus

use strict;
use lib ("/home/joshua/projects/sisyphus/lib/");

use AnyEvent::Strict;
use Sisyphus::Connector;
use JSON;

# ok so, we need a real client lib. we're starting to do intense
# things in this "example script", including maintaining a consistent
# hash cluster

use Set::ConsistentHash;
use String::CRC32;

my $set = Set::ConsistentHash->new;
$set->set_hash_func(\&crc32);

my $cluster = {
	"chunk0" => {
		ip => "192.168.170.127", # running on cazon
		port => "8889",
		db => "keyval",
		user => "keyval",
		pw => "roses",
	},

	"chunk1" => {		# running locally on piper
		ip => "127.0.0.1",	
		port => "8889",
		db => "keyval",
		user => "keyval",
		pw => "roses",
	},
};

my $targets = {
    "chunk0" => 1,
    "chunk1" => 1,
};

$set->modify_targets( %$targets );

foreach my $chunk ($set->targets()) {
	my $serv = $cluster->{$chunk};

	my $ac  = new Sisyphus::Connector;
	$ac->{host} = $serv->{ip};
	$ac->{port} = $serv->{port};
	$ac->{protocolName} = "Trivial";

	$ac->{app_callback} = sub {
		my $message = shift;
		print "i received a message from $chunk:\n";
		print $message . "\n";
	};

	$cluster->{$chunk}->{ac} = $ac;

	my $cv = AnyEvent->condvar;
	$ac->connect(sub { print STDERR "connected.\n"; $cv->send; });
	$cv->recv;
}

foreach my $key (qw/ringo john joshua jones frank dweezil moonunit dave jimmy miles jerry phil bob bill zakhir/) {
	my $serv =  $set->get_target($key);
	print "going to $serv ($cluster->{$serv}->{ip}) for $key\n";
	my $ac = $cluster->{$serv}->{ac};

	# set a record
	my $o = {
	command => "set",
	key => $key,
	data => {
		first => "John",
		last => "Bonham",
		band => "Zeppelin",
		albums => [
			"Houses of the Holy","IV"
		],
	},
	};
			
	$ac->send(to_json($o));

	# get a record
	# in this case, we add a request_id, which will be returned to us
	# in the result set. which allows us to keep track of queries and they
	# return. probably, ideally, we'd be able to spec a callback here instead
	my $json = to_json($o = {
		command => "get",
		key => $key,
		request_id => 1,
	});

	$ac->send($json);
}

AnyEvent->condvar->recv;
