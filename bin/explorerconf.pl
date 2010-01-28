package Config;

$applog = "mykeyv-app";
$httplog = "mykeyv-http";
$port = 8886;
$ip  = "127.0.0.1";

# the cluster, that the client sees
# confused. i think the client doesn't care about db names et al
$cluster = [
	{
		ip => "127.0.0.1",
		port => "8000",
		db => "KeyValue",
		user => "KeyValue",
		pw => "KeyValuePass",
		table => "KeyValue0",
	},
	{
		ip => "127.0.0.1",
		port => "8001",
		db => "KeyValue",
		user => "KeyValue",
		pw => "KeyValuePass",
		table => "KeyValue1",
	},
	{
		ip => "127.0.0.1",
		port => "8002",
		db => "KeyValue",
		user => "KeyValue",
		pw => "KeyValuePass",
		table => "KeyValue2",
	},
	{
		ip => "127.0.0.1",
		port => "8003",
		db => "KeyValue",
		user => "KeyValue",
		pw => "KeyValuePass",
		table => "KeyValue3",
	},
];	
