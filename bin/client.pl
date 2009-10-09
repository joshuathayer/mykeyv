# example mykeyv client using sisyphus

use strict;
use lib ("/Users/joshua/projects/sisyphus/lib/");

use AnyEvent::Strict;
use Sisyphus::Connector;
use JSON;

# ok so, we need a real client lib. we're starting to do intense
# things in this "example script", including maintaining a consistent
# hash cluster

use Set::ConsistentHash;
use String::CRC32;

BEGIN {
	if ($#ARGV < 0) {
		print "Usage: $0 PATH_TO_CONFFILE\n";
		exit;
	}
}
require $ARGV[0];
my $cluster = $Config::cluster;

my $set = Set::ConsistentHash->new;
$set->set_hash_func(\&crc32);

# set up hash of cluster elements, for Consistent to work with
my $i = 0; my $targets;
foreach my $t (@$cluster) {
	$targets->{$i} = 1;
	$i += 1;
}

$set->modify_targets( %$targets );

foreach my $target ($set->targets()) {
	my $serv = $cluster->[$target];

	my $ac  = new Sisyphus::Connector;
	$ac->{host} = $serv->{ip};
	$ac->{port} = $serv->{port};
	$ac->{protocolName} = "Trivial";

	$ac->{app_callback} = sub {
		my $message = shift;
		print "i received a message from $target:\n";
		print $message . "\n";
	};

	$cluster->[$target]->{ac} = $ac;

	my $cv = AnyEvent->condvar;
	$ac->connect(sub { print STDERR "connected.\n"; $cv->send; });
	$cv->recv;
}

foreach my $key (qw/ringo john joshua jones frank dweezil moonunit dave jimmy miles jerry phil bob bill zakhir/) {
	my $serv =  $set->get_target($key);
	print "going to $serv ($cluster->[$serv]->{ip}) for $key\n";
	my $ac = $cluster->[$serv]->{ac};

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
