#!/usr/bin/perl
use utf8;
use warnings;
use strict;
use DBI;
use LWP;
use Data::Dumper;
use LWP::UserAgent::DNS::Hosts;
use IO::Compress::Gzip qw(gzip $GzipError) ;
use threads;
use threads::shared;
use Thread::Queue;
use Thread::Semaphore;
use POSIX qw(strftime);
use Time::HiRes qw(time sleep);
use JSON;
use Text::CSV;
use Config::Simple;

# --- Simple usage. Loads the config. file into a hash:
my %config;
Config::Simple->import_from('config.ini', \%config);
	
my $debug = 1;
my $enc_accept = 'application/json';
my $min_delay = 0.50;
my $rnd_delay = 1.25;

#mysql:
my $user = $config{'default.user'};
my $password = $config{'default.password'};
my $database = $config{'default.database'};
my $hostname = $config{'default.hostname'};
my $port     = $config{'default.port'};

#API data
my $AVAPIKEY = $config{'default.AVAPIKEY'};

my $AVURL = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&outputsize=full&apikey='. $AVAPIKEY . '&datatype=csv&symbol=';

my $crawler_max_threads = 20; # number of parallel workers at the beginning
my $crawler_min_threads = 4;  # decreased number of parallel workers in case of errors
my $crawler_max_parallel_requests = 10;  # limit number of parallel web requests
my $crawler_max_req_rate = 0.46;  # limit number of parallel requests per second via rate value
my $crawler_max_failed = 6;  # after so many failed requests, the number of workers will be decreased to minimum
my $crawler_max_attempts = 2;  # after so many tries and failed attempts the worker will give up requesting a symbol
my $crawler_req_timeout = 30;  # HTTP timeout for LWP
my $crawler_max_secs = 32;  # maximum delay and also a second half of a minute when wait_next_minute function will be invoked
my $crawler_min_bytes = 500;  # minimum saved file size to determine a successful completion
my $crawler_pause_ALL :shared = 0;  # a universal pause button for all threads preventing placing new requests (temporarily)
my $crawler_req_cnt :shared = 0;  # number of requests overall
my $crawler_req_err :shared = 0;  # number of failed requests

my $crawler_maxreqs_limit = Thread::Semaphore->new( $crawler_max_parallel_requests ) ;
my $crawler_begin = time;  # record starting time for request rate calculation
my $worker_id = 0;  # will be assigned with worker thread id inside threads, 0 for main thread execution

my $dsn = "DBI:mysql:database=$database;host=$hostname;port=$port";


sub dtlog {
   strftime( '%a %b %d %H:%M:%S ', localtime ) . '<#' . sprintf( '%02d', $worker_id ) . '> ';
}

sub secs {
   strftime( '%S', localtime );
}

sub wait_next_minute {
   my $begin = secs();
   return unless $begin;
   while ( secs() >= $begin ) {
      sleep( $min_delay );
   }
}

my $hostsbyip = {};

sub alternateIP {
   my ( $hostname ) = @_;
   if ( ! exists $hostsbyip->{$hostname} ) {
      my ( $name, $aliases, $addrtype, $length, @addrs ) = gethostbyname( $hostname );
      @{$hostsbyip->{$hostname}} = map { join( '.', unpack( 'C4', $_ ) ) } @addrs;
   }
   my $randomIPhost = $hostsbyip->{$hostname}[int(rand( scalar @{$hostsbyip->{$hostname}} ))];
   LWP::UserAgent::DNS::Hosts->register_host( $hostname => $randomIPhost );
   LWP::UserAgent::DNS::Hosts->enable_override;
}



sub failed {
   my ( $reason, $tfile, $response ) = @_;
   {
      lock( $crawler_req_err );
      $crawler_req_err++;
   }
   print dtlog . $reason . ', resp: ' . $response->code . "\n";
   unlink $tfile;
   if ( ! $crawler_pause_ALL ) {
      lock( $crawler_pause_ALL );
      $crawler_pause_ALL = 1;
      wait_next_minute() if secs() > $crawler_max_secs;
      $crawler_pause_ALL = 0;
   }
}

sub download {
   my ( $browser, $data ) = @_;
   #print Dumper $data;
   my $symbol = $data->{symbol};
   my $dbh = DBI->connect($dsn, $user, $password) or die "could not connect $! ($dsn / $user / $password)";

   $crawler_maxreqs_limit->down; # wait for our turn, or lower the semaphore
   my $delaytime = $min_delay + rand( $crawler_req_err ) ;
   my $persec = 0;
   do { # initial delay, then check our pause ALL button and current rate of requests, if we can work
      sleep $delaytime;
   } until ( ! $crawler_pause_ALL && ( $persec = $crawler_req_cnt / ( time - $crawler_begin ) ) < $crawler_max_req_rate );
   { # counting outgoing requests via shared variable 
      lock( $crawler_req_cnt );
      $crawler_req_cnt++;
   }
   my ( $hostname ) = ( $AVURL =~ /\:\/\/(.*?)\// );
   alternateIP( $hostname );
   $symbol =~ s/\s/\./g;
   my $response = $browser->get( $AVURL . $symbol, 'Accept-Encoding' => $enc_accept );
   my $dlfile = $symbol . '.csv.gz' ;
   my $tfile = $dlfile . $$ . '.tmp' ;
   $crawler_maxreqs_limit->up; # raise the semaphore, upon get completion, allowing other threads to place requests 
   if ( $response->code ne '200' ) {
      failed( 'Failed, bad or empty for symbol $symbol'. $response );
      return;
   };
 
   my $message = $response->decoded_content;	
   my @lines = split "\n", $message;
   my $header_found=0;
   
   $dbh->{AutoCommit} = 0;
   my $sql_move = "insert into StockValueChange (id,timestamp,open,high,low,close,adjusted_close,volume,dividend_amount,split_coefficient) VALUES (?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE open=?,high=?,low=?,close=?,adjusted_close=?,volume=?,dividend_amount=?,split_coefficient=?";
   my $sth1 = $dbh->prepare_cached( $sql_move );
   if (scalar @lines > 10 ) {
   
	   
	   foreach my $line (@lines){
		   if (!$header_found){
			$header_found = 1;
			next;
		   }
		   my ($timestamp,$open,$high,$low,$close,$adjusted_close,$volume,$dividend_amount,$split_coefficient) = split "," , $line;
		   $sth1->execute( $data->{id},$timestamp,$open,$high,$low,$close,$adjusted_close,$volume,$dividend_amount,$split_coefficient,$open,$high,$low,$close,$adjusted_close,$volume,$dividend_amount,$split_coefficient);
	   }
	   $sth1->finish;
	   my $sql_update_finish = "update StockInfo SET last_update_time=FROM_UNIXTIME(?) where id=?";
	   my $now = time;

	   my $sth3 = $dbh->prepare_cached( $sql_update_finish ); 
	   $sth3->execute($now,$data->{id});
	   $dbh->{AutoCommit} = 1;
	   print dtlog . 'Symbol:' . $symbol. " inserted with ".scalar (@lines)." lines!"."\n";	
   }	
   else {
      print dtlog . 'Symbol:' . $symbol. " not enough items (".scalar (@lines)." lines)!"."\n";
	  return 0;
   }
	
   
   return 1;
}
#

$| = 1;
srand;
my $dbh = DBI->connect($dsn, $user, $password)  or die "could not connect $! ($dsn / $user / $password)";

my $sth = $dbh->prepare('SELECT id,symbol FROM StockInfo WHERE symbol is not NULL and last_update_time < NOW()- INTERVAL 1 DAY ')
        or die "prepare statement failed: $dbh->errstr()";
$sth->execute() or die "execution failed: $dbh->errstr()";
print $sth->rows . " rows found.\n";
my @stocks;
my $newjobs = Thread::Queue->new();
while (my $ref = $sth->fetchrow_hashref()) {
		my %data;
		$data{symbol} =$ref->{'symbol'};
		$data{id} =$ref->{'id'};
        push @stocks, $ref->{'symbol'};
		$newjobs->enqueue( \%data );
}
$sth->finish;
	

my $jobs_counter = scalar @stocks;

my $max_parallel_wrks = ( $crawler_max_threads < $jobs_counter ) ? $crawler_max_threads : $jobs_counter ;
my @workers = map {
   $worker_id++;
   threads->create( 
     sub {   
        my $browser = LWP::UserAgent->new();
		
        $browser->timeout( $crawler_req_timeout );
        sleep rand( $rnd_delay + $worker_id );
        while ( defined ( my $item = $newjobs->dequeue ) ) { 
            threads->yield;
            my $ntry = 1;
            do {
               print dtlog . 'Try ' . $ntry . ' (' . $item->{symbol} . ')' . "\n";
            } until ( download( $browser, $item ) || ++$ntry > $crawler_max_attempts );
            return if $worker_id > $crawler_min_threads && $crawler_max_failed < $crawler_req_err ;
        }  
     } 
   );
} 1 .. $max_parallel_wrks;
$worker_id = 0;
sleep $min_delay;
$newjobs->enqueue( undef ) for @workers;  
$_->join() for @workers;
print dtlog . 'DONE.' . "\n";
exit 0;
