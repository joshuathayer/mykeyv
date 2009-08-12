use lib ("/home/joshua/projects/sisyphus/lib");
use lib ("/home/joshua/projects/keyv/lib");

use Keyv;
use Data::Dumper;

# set up keyvalue handle
my $kv = Keyv->new({
	'host' => "127.0.0.1",
	'port' => 3306,
	'user' => 'keyvalue',
	'pw' => 'roses',
	'db' => 'keyvalue',
});

# a test object to store
my $object = {
	first => "Ringo",
	last => "Starr",
	albums => ["Abbey Road","Revolver","Rubber Soul"],
	rank => 3,
}; 

# synchronous (blocking) set
$kv->setSync( "ringo", $object );

# synchronous (blocking) get
my $v = $kv->getSync( "ringo" );
print "got ringo row:\n";
print Dumper $v;

# asynchronous (nonblocking) get
$cv = AnyEvent->condvar;
$kv->get(
	"ringo", sub { $v = shift; print "in callback...\n"; $cv->send(1); }
);
print "waiting for async...\n";
$cv->recv;

print "got ringo row:\n";
print Dumper $v;
