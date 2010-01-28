package ExplorerApplication;

use strict;
use base 'Sisyphus::Application';

use Data::Dumper;
use Devel::Leak;
use JSON;

my $handle;

my $responses;

sub new {
	my $class = shift;
    my ($kvc, $httplog, $applog) = @_;

	my $self = { };
    $self->{kvc} = $kvc;

    $self->{js} = '';
    $self->{jq} = '';
    open(FH, "js/JSONeditor.js");
    while(<FH>) { $self->{js} .= $_; }
    close FH;

    open(FH, "js/jquery-1.3.2.min.js");
    while(<FH>) { $self->{jq} .= $_; }
    close FH;

    foreach my $file (<static/treeBuilderImages/*>) {
        next unless $file =~ /gif/;
        print "reading $file\n";
        open(FH, $file);
        my @f = split(/\//,$file);
        $file = $f[-1];
        $self->{static}->{$file} = '';
        while(<FH>) { $self->{static}->{$file} .= $_; }
        close FH;
    }

	return(bless($self, $class));
}

sub new_connection {
	my $self = shift;
}

sub remote_closed {
	my ($self, $host, $port, $fh) = @_;
	delete $responses->{$fh};
}

# passed the host and port of the requesting machine,
# and an array of data as passed by the protocol module
sub message {
	my ($self, $host, $port, $dat, $fh, $stash) = @_;

	my $req = $dat->[0];

	# $req is an AE::HTTPD::Request instance
	# Request class has methods for talking to the AE loop
	# but we'll do it by hand here to demonstrate the use of Sisyphus
	my $meth = $req->method();
	my $url= $req->url;
	my $params = $req->vars;
	my $headers = $req->headers();

    my $ret = "<html><head><title>mykeyv explorer</title></head><body>";
    if ($url =~ /JSONeditor\.js/) {
        print "request for json editor\n";
         $responses->{$fh} =
		        [200, "OK", {"Content-type" => "text/javascript",}, $self->{js}];
	    $self->{client_callback}->([$fh]);
    } elsif ($url =~ /treeBuilderImages/) {
        my @f = split(/\//,$url);
        my $file = $f[-1];
        print "image req $url - $file\n";
         $responses->{$fh} =
		        [200, "OK", {"Content-type" => "image/gif",}, $self->{static}->{$file}];
	    $self->{client_callback}->([$fh]);
    } elsif ($url =~ /jquery-1.3.2.min.js/) {
        print "request for jquery...\n";
         $responses->{$fh} =
		        [200, "OK", {"Content-type" => "text/javascript",}, $self->{jq}];
	    $self->{client_callback}->([$fh]);
    } elsif ($url =~ /\/json\/(.*)/) {
        print "JSON REQUEST FOR $1 (from $url)\n";
        my $k = $1;
        if ($1 eq '') {
            $self->{kvc}->list(sub {
                my $list = shift;
                my $j = JSON::to_json($list);
    	        $responses->{$fh} =
	    	        [200, "OK", {"Content-type" => "application/json",}, $j];
	            $self->{client_callback}->([$fh]);
            });
        } else {
            $self->{kvc}->get($k, sub {
                my $item = shift;
                my $j = JSON::to_json($item->{data});
    	        $responses->{$fh} =
	    	        [200, "OK", {"Content-type" => "application/json",}, $j];
	            $self->{client_callback}->([$fh]);
            });
        }   
    } elsif ($url =~ /\/see\/(.*)/) {
        my $k = $1;
        my $ret = <<QQ;
        <!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN" "http://www.w3.org/TR/REC-html40/loose.dtd">
            <html>
            <head>

<script src="/js/JSONeditor.js"></script>
<script src="/scripts/jquery-1.3.2.min.js"></script>

    <script>
    onload=function(){

        \$.getJSON('/json/$k', function(data) {
            JSONeditor.start('tree','jform',data,false)
            Opera=(navigator.userAgent.toLowerCase().indexOf("opera")!=-1)
            Safari=(navigator.userAgent.toLowerCase().indexOf("safari")!=-1)
            Explorer=(document.all && (!(Opera || Safari)))
            Explorer7=(Explorer && (navigator.userAgent.indexOf("MSIE 7.0")>=0))
                                        
            if(Explorer7 && location.href.indexOf("file:")!=0){prompt("This is just to get input boxes started in IE7 - who deems them unsecure.","I like input boxes...")}
        });

    }
</script>
    </head>
    <body>
    <div style="position:absolute;top:10px;left:10px" id="tree"></div>
    <div style="position:absolute;top:10px;left:400px" id="jform"></div>
    <div style="position:absolute;top:13px;left:750px;font-family:Verdana,Arial,Helvetica;font-size:11px"><a href="http://www.thomasfrank.se/json_editor.html" target="_blank">Help/info</a></div>
    </body>
    </html>
QQ
    	    $responses->{$fh} =
	    	    [200, "OK", {"Content-type" => "text/html",}, "$ret"];
	        $self->{client_callback}->([$fh]);	

    } else {

        $self->{kvc}->list(sub {
            my $list = shift;

            foreach my $h (keys(%$list)) {
                $ret .= "<h2>$h</h2>\n";
                $ret .= "<ul>";
                foreach my $item (@{$list->{$h}}) {
                    $ret .= "<li><a href=\"see/$item\">$item</a></li>\n";
                }
                $ret .= "</ul>";
            }
   
            $ret .= "</body></html>";

            $responses->{$fh} =
                [200, "OK", {"Content-type" => "text/html",}, "$ret"];
            $self->{client_callback}->([$fh]);

        });

    }

	return undef;
}

sub get_data {
	my ($self, $fh) = @_;

	unless ($responses->{$fh}) { return; }
	my $v = $responses->{$fh};
	delete $responses->{$fh};
	return $v;
}

1;
