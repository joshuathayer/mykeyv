use lib ("/home/joshua/projects/sisyphus/lib");
use lib ("/home/joshua/projects/keyv/lib");

use Keyv;
use Data::Dumper;

my $kv = Keyv->new({
	'host' => "127.0.0.1",
	'port' => 3306,
	'user' => 'keyvalue',
	'pw' => 'roses',
	'db' => 'keyvalue',
});

my $object = {
	first => "Ringo",
	last => "Starr",
	albums => ["Abbey Road","Revolver","Rubber Soul"],
	rank => 3,
}; 

my $v;
my $cv = AnyEvent->condvar;
$kv->set(
	"ringo", $object, sub { print "in control script. set.\n"; $cv->send(); }
);
$cv->recv;

$cv = AnyEvent->condvar;
$kv->get(
	"ringo", sub { $v = shift; $cv->send(1); }
);
$cv->recv;
print "got ringo row:\n";
print Dumper $v;

$cv = AnyEvent->condvar;
$kv->get(
	"bonham", sub { $v = shift; $cv->send(1); }
);
$cv->recv;

print "got bonham row:\n";
print Dumper $v;
