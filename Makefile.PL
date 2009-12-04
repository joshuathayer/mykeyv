use inc::Module::Install;

name     'Mykeyv';
all_from 'lib/Mykeyv/Mykeyv.pm';
author   'Joshua Thayer <joshuamilesthayer@gmail.com>';

build_requires 'Test::More';

requires 'Net::Server::Daemonize' => 0;
requires 'Set::ConsistentHash'	=> 0;
requires 'String::CRC32'	=> 0;
requires 'Data::HexDump'	=> 0;
requires 'File::Lockfile'	=> 0;

license		'perl';

auto_install;

WriteAll;

