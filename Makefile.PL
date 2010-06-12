use inc::Module::Install;

name         'Mykeyv';
perl_version '5.005';
author       'Joshua Thayer <joshuamilesthayer@gmail.com>';
license      'perl';
all_from     'lib/Mykeyv/Mykeyv.pm';

build_requires 'Test::More';

requires 'Net::Server::Daemonize' => 0;
requires 'Set::ConsistentHash'    => 0;
requires 'String::CRC32'          => 0;
requires 'Data::HexDump'          => 0;
requires 'File::Lockfile'         => 0;
requires 'Log::Syslog::Fast'         => 0;
requires 'URI::Escape'			=> 0;

auto_install;

WriteAll;

