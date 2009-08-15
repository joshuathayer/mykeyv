use Set::ConsistentHash;
use Data::Dumper;
use String::CRC32;
use strict;

my $set = Set::ConsistentHash->new;
$set->set_hash_func(\&crc32);

sub generate_random_string
{
	my $length_of_randomstring=shift;# the length of 
			 # the random string to generate

	my @chars=('a'..'z','A'..'Y','0'..'9','_');
	my $random_string;
	foreach (1..$length_of_randomstring) 
	{
		# rand @chars will generate a random 
		# number between 0 and scalar @chars
		$random_string.=$chars[rand @chars];
	}
	return $random_string;
}

my $targets = {
	"chunk0" => 1,
	"chunk1" => 1,
	"chunk2" => 1,
	"chunk3" => 1,
};

$set->modify_targets( %$targets );
print Dumper $set->percent_weight("chunk0");
my $i = 0;
my $w = {
	chunk0 => 0,
	chunk1 => 0,
	chunk3 => 0,
	chunk3 => 0,
};

my $res;

while ($i < 1000) {
	my $str = generate_random_string(10);
	my $buk =  $set->get_target($str);

	print "$str -> $buk\n";
	$res->{$str} = $buk;
	$w->{ $buk } +=1;
	$i += 1;
}

my $targets = {
	"chunk0" => 1,
	"chunk1" => 1,
	"chunk2" => 1,
	"chunk3" => 1,
	"chunk4" => 1,
};

$set->modify_targets( %$targets );
my $changed = 0;
foreach my $k (keys %{$res}) {
	my $buk = $res->{$k};
	my $nuk = $set->get_target($k);

	if ($nuk ne $buk) {
		$changed += 1;
		print "$k now $nuk (was $buk)\n";
	} else {
		print "$k still $nuk\n";
	}
}
print "$changed items needed rehashing.\n";
#print Dumper $w;
