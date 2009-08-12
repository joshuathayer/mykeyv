# example mykeyv client using sisyphus

use strict;
use lib ("/home/joshua/projects/sisyphus/lib/");

use AnyEvent::Strict;
use Sisyphus::Connector;
use JSON;

my $ac  = new Sisyphus::Connector;
$ac->{host} = "127.0.0.1";
$ac->{port} = 8889;
$ac->{protocolName} = "Trivial";

$ac->{app_callback} = sub {
	my $message = shift;
	print "i received a message:\n";
	print $message . "\n";
};

my $cv = AnyEvent->condvar;
$ac->connect(sub { print STDERR "connected.\n"; $cv->send; });
$cv->recv;

# set a record
my $o = {
	command => "set",
	key => "bonham2",
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
	key => "bonham",
	request_id => 1,
});

$ac->send($json);

AnyEvent->condvar->recv;
