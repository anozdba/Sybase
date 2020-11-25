#!/usr/bin/perl
# --------------------------------------------------------------------
# checkLatency.pl
#
# $Id: checkLatency.pl,v 1.34 2019/08/23 05:15:33 db2admin Exp db2admin $
#
# Description:
# Script to check the latency of the replication link using rs_ticket_history
#
# Usage:
#   checkLatency.pl -S <Server>
#
# $Name:  $
#
# ChangeLog:
# $Log: checkLatency.pl,v $
# Revision 1.34  2019/08/23 05:15:33  db2admin
# make sure waiting on non-existent IP address
#
# Revision 1.33  2019/01/25 04:08:18  db2admin
# correct bug with previous change where replace done incorrectly
#
# Revision 1.32  2019/01/25 03:12:40  db2admin
# adjust commonFunctions.pm parameter importing to match module definition
#
# Revision 1.31  2018/03/09 04:58:49  db2admin
# add in MSA_check as a valid heartbeat
#
# Revision 1.30  2017/09/18 21:43:54  db2admin
# added in the thresholds that are being test against in the error message
#
# Revision 1.29  2017/06/26 05:05:45  db2admin
# correct -T parameter
#
# Revision 1.28  2017/06/22 04:48:31  db2admin
# another try at calculated frequency
#
# Revision 1.27  2017/06/21 09:39:34  db2admin
# adjust calculated latency for 'latency too long' alerts
#
# Revision 1.26  2017/06/21 05:28:45  db2admin
# 1. add in 'DB Offline' state which sometimes appears during a load
# 2. Add in option to specify the allowable wait period for heartbeat rows
# 3. clean up unusual message file
# 4. set default for latency threshold to 60 seconds
# 5. create variable to hold acceptable state values
#
# Revision 1.25  2017/06/21 04:18:13  db2admin
# have another go at calculating latency
#
# Revision 1.24  2017/06/21 02:03:35  db2admin
# reverse calculatedLatency change
#
# Revision 1.23  2017/06/21 01:29:53  db2admin
# correct compile error
#
# Revision 1.22  2017/06/21 01:27:37  db2admin
# always calculate latency
#
# Revision 1.21  2017/06/19 23:16:08  db2admin
# 1. improve clarity of code
# 2. Include file to hold unexepcted output from the sybase commands to aid in problem diagnosis
#
# Revision 1.20  2017/06/19 02:31:27  db2admin
# ensure date is in a loadable format
#
# Revision 1.19  2017/06/19 00:37:28  db2admin
# add timestamps to server unavailable messages
#
# Revision 1.18  2017/06/19 00:00:51  db2admin
# calculate latency for LATENCY_TOO_OLD as 'current time' - 'last heartbeat start'
#
# Revision 1.17  2017/06/15 05:55:57  db2admin
# also output the time of the snapshot
#
# Revision 1.16  2017/06/14 05:18:45  db2admin
# add in summary line for LATENCY alert
#
# Revision 1.15  2017/06/14 03:46:24  db2admin
# add in processig to identify
#  1. dataabse being loaded
#  2. server down
#  3. database not found
#
# Revision 1.13  2017/06/13 05:25:55  db2admin
# 1. Correct compile error
# 2. fix up email heading
#
# Revision 1.12  2017/06/13 02:44:30  db2admin
# 1. Correct saved last alert times
# 2. correct timing issues
# 3. corrected email formatting
# 4. improved elapsed time literals
#
# Revision 1.11  2017/06/08 05:24:56  db2admin
# this time set the value of the $header value correctly
#
# Revision 1.10  2017/06/08 04:02:51  db2admin
# ensure that the header is printed out for each iteration
#
# Revision 1.9  2017/06/08 00:47:42  db2admin
# escape the @ in the scp address
#
# Revision 1.8  2017/06/08 00:36:49  db2admin
# add in a copy of the latency to 192.168.1.1
#
# Revision 1.7  2017/06/08 00:15:32  db2admin
# 1. Correct the default email address
# 2. Change the help description of the script
#
# Revision 1.6  2017/06/07 05:32:27  db2admin
# straighten up the email
#
# Revision 1.4  2017/03/03 12:29:19  db2admin
# correct some spelling mistakes
# add in note about last alert file
#
# Revision 1.3  2017/03/03 12:26:46  db2admin
# correct the names of the temporary files
#
# Revision 1.2  2017/03/03 04:32:34  db2admin
# Working version
#
# Revision 1.1  2017/03/03 00:23:48  db2admin
# Initial revision
#
# --------------------------------------------------------------------

use strict;

my $debugLevel = 0;
my %monthNumber;

my $ID = '$Id: checkLatency.pl,v 1.34 2019/08/23 05:15:33 db2admin Exp db2admin $';
my @V = split(/ /,$ID);
my $Version=$V[2];
my $Changed="$V[3] $V[4]";

my $machine;   # machine we are running on
my $OS;        # OS running on
my $scriptDir; # directory the script ois running out of
my $tmp ;
my $machine_info;
my @mach_info;
my $user = 'Unknown';
my $dirsep;
my $timeString;
my $exitCode = 0;
my %lastAlertTime = ();
my $lastAlertAlarmKey;
my %currentAlerts = ();
my $TS;
my $warn = 'CRITICAL ';
my $latency = '';
my %lastRun = ();

BEGIN {
  if ( $^O eq "MSWin32") {
    $machine = `hostname`;
    $OS = "Windows";
    $scriptDir = 'c:\udbdba\scrxipts';
    $tmp = rindex($0,'\\');
    $user = $ENV{'USERNAME'};
    if ($tmp > -1) {
      $scriptDir = substr($0,0,$tmp+1)  ;
    }
    $dirsep = '\\';
  }
  else {
    $machine = `uname -n`;
    $machine_info = `uname -a`;
    @mach_info = split(/\s+/,$machine_info);
    $OS = $mach_info[0] . " " . $mach_info[2];
    $scriptDir = "scripts";
    $user = `id | cut -d '(' -f 2 | cut -d ')' -f 1`;
    $tmp = rindex($0,'/');
    if ($tmp > -1) {
      $scriptDir = substr($0,0,$tmp+1)  ;
    }
    $dirsep = '/';
  }
}

use lib "$scriptDir";

use commonFunctions qw(getOpt myDate trim $getOpt_optName $getOpt_optValue @myDate_ReturnDesc $cF_debugLevel timeDiff);

# Global Variables

my $tmpIn = "$scriptDir/checkLatency.sql";
my $tmpRep = "/tmp/checkLatency_$$.rep"; # specific to this run
my $tmpOut = "/tmp/checkLatency_$$.out"; # specific to this run
my $emailFile = '';
my $lastAlertFile = '';
my $exclDBFile = '';
my $checkLatMsgFile = '';
my $reportingServerFile = '';
my $currentSection = '';
my $header = 1;
my ($Info_ID_Type,$Info_Q_identifier,$Duplicates,$Writes,$Reads,$Bytes,$B_Writes,$B_Filled,$B_Reads,$B_Cache,$Save_Int_Seg,$First_Seg_Block,$Last_Seg_Block,$Next_Read,$Readers,$Truncs);
my $databaseNotMonitored = 1;
my %latencyHold = ();
my $serverDown = 0;
my $beingLoaded = 0;
my $offline = 0;
my $cantConnect = 0;
my %excludeList = ();

# DB states that wont raise an alert
my %OK_State = ();
$OK_State{'DB_Being_Loaded'} = 1;
$OK_State{'DB_Offline'} = 1;

my $PWDLoc = $ENV{'DBA_SYBMAINT_PWD_FILE'};
my $tmpPWD = `cat $PWDLoc`;
chomp $tmpPWD;

# options set on the command line
my $screenOnly = 0;
my $silent = "No";
my $server = '';
my $number = 1;
my $wait = 60;
my $waitMS = 60 * 1000;
my $heartbeatFreq = 300;
my $waitThreshold = 600;
my $threshold = 60;    # maximum number of latency seconds allowed before an alert is raised
my $email = 'MPL_IT_Sybase_DBA@KAGJCM.com.au';
my $delay = 240;
my $databases = '';  # list of databases to be processed
my $databaseFile = ''; # file containing databases to be processed
my $fileSuff = '';


# ----------------------------------------------------------------------------------
# Subroutines and functions ......
# ----------------------------------------------------------------------------------

sub by_key {
  $a cmp $b ;
}

sub usage {
  if ( $#_ > -1 ) {
    if ( trim("$_[0]") ne "" ) {
      print "\n$_[0]\n\n";
    }
  }


  print "Usage: $0 -?hs [-v[v]] [-g <literal>] [-S <server name>] [-n <number>] [-w <number>] [-t <seconds>] [-T <seconds>]
                        [-d <number>] [-e <email address>] [-x] [-D <databases>] [-f <file of databases>] [-W <heartbeat frequency>]

       Script to check the latency occurring for a replicated copy of a database

       Version $Version Last Changed on $Changed (UTC)

       -h or -?        : This help message
       -s              : Silent mode (no parameter information will be displayed)
       -S              : server to connect to
       -g              : database group. literal suffixed to files to make this run unique
       -n              : number of iterations (default 1)
       -w              : wait between iterations (in seconds, default 60)
       -W              : expected heartbeat frequency (in seconds, default 300)
       -t              : display entries where latency is greater than this number of seconds (default 60)
                         (when set ONLY iterations where latency is greater than this value will be displayed)
       -T              : raise an alert if heartbeats are more than this number of seconds apart (default 600)
       -d              : alerting delay in minutes to occur for delays between alerts for a database (defaults to 240)
       -D              : databases to monitor (separated by commas) [defaults to master if -D and -f not supplied]
       -f              : file of databases to monitor (-D databases will be added to this list)
       -e              : email address to send alerts to (defaults to MPL_IT_Sybase_DBA\@KAGJCM.com.au)
       -x              : dont send emails just display to screen
                         (this will also prevent the last alert timestamps from being updated)
       -v              : debug level

  Note: the Last alert timestamps are held in file last_alert.txt in the directory this script is run from

  Exit codes: 0 - all queues are empty
              2 - some queues are not empty
              4 - some queues have exceeded the Mb threshold

\n";

}

sub sendEmail {

  # send the email alerts, reset the lastAlertTimes and then save them

  if ( $email eq '' ) { return; } # if no email address specified then dont send the email

  if ( $screenOnly ) { return; } # the -x option has been set so no emails

  if ( $debugLevel > 0 ) { print "**** Sending the email \n"; }

  if ( ! open(EMAIL, ">$emailFile") ) {
    die "Unable to open $emailFile\n$!\n";
  }
  else { # create the email .....
    # create the commands to send the email

    print EMAIL "#!/bin/bash\n\n";
    print EMAIL '(' . "\n";
    print EMAIL "  echo 'To:$email'\n";
    print EMAIL '  echo \'From:do-not-reply@KAGJCM.com.au\'' . "\n";
    print EMAIL "  echo 'Subject: ${warn} " . substr($TS,0,16) . ": $server - Latency Issues'\n";

    print EMAIL "  echo 'MIME-Version: 1.0'\n";
    print EMAIL '  echo \'Content-Type: multipart/mixed; boundary="-q1w2e3r4t5"' . "'\n";

    print EMAIL '  echo \'---q1w2e3r4t5' . "'\n";
    print EMAIL '  echo \'Content-type: text/html' . "'\n";
    print EMAIL '  echo \'<html>' . "'\n";
    print EMAIL '  echo \'<head><title>Latency Issues on $server</title></head>' . "'\n";
    print EMAIL '  echo \'<body style="font-family:arial">' . "'\n";
    print EMAIL '  echo \'<h4><u>' . "Latency issues Identified on $server at $TS" . '</u></h4>' . "'\n";
    print EMAIL "  echo 'The following report was produced on $server at $TS <BR><BR>'\n";
    print EMAIL "  echo 'The report was produced because:<BR><BR>'\n";

    foreach my $tmpKey (sort by_key keys %currentAlerts) {
      my ($myDB, $myAlert) = split (':', $tmpKey);

      # NOTE: Database being loaded is not a reason to alert

      if ( $myAlert eq 'No_Heartbeat_Data' ) {
        print EMAIL "  echo '    - Either DB $myDB does not have a heartbeat tran or could not connect to DB <BR>'\n";
      }
      elsif ( $myAlert eq 'DB_Being_Loaded' ) { # shouldnt get here if this is the only thing happening
        # print EMAIL "  echo '    - Database $myDB was being loaded<BR>'\n";
      }
      elsif ( $myAlert eq 'DB_Offline' ) { # shouldnt get here if this is the only thing happening
        # print EMAIL "  echo '    - Database $myDB was Offline<BR>'\n";
      }
      elsif ( $myAlert eq 'DB_Not_Found' ) {
        print EMAIL "  echo '    - Database $myDB was not found<BR>'\n";
      }
      elsif ( $myAlert eq 'Last_LATENCY_CHK' ) {
        my $lastAlertLit = "There has been no previous alert for Last_LATENCY_CHK";
        if ( defined($lastAlertTime{"$myDB:Last_LATENCY_CHK"}) ) {
          $lastAlertLit = "The last alert for Last_LATENCY_CHK was at " . $lastAlertTime{"$myDB:Last_LATENCY_CHK"};
        }
        print EMAIL "  echo '    - data in the rs_ticket_history table indicates that the last latency check for $myDB was done " . elapsedLit($lastRun{$myDB}) . " ago. $lastAlertLit. <BR>'\n";
      }
      elsif ( $myAlert eq 'LATENCY' ) { # the latency is the issue
        my $lastAlertLit = "There has been no previous alert for latency";
        if ( defined($lastAlertTime{"$myDB:LATENCY"}) ) {
          $lastAlertLit = "The last alert for latency was at " . $lastAlertTime{"$myDB:LATENCY"};
        }
        print EMAIL "  echo '    - data in the rs_ticket_history table indicates that latency for $myDB is an issue. $lastAlertLit. <BR>'\n";
      }
      elsif ( $myAlert eq '' ) {
        my $lastLatencyAlertLit = "There has been no previous alert for LATENCY";
        if ( defined($lastAlertTime{"$myDB:LATENCY"}) ) {
          $lastLatencyAlertLit = "The last alert for LATENCY was at " . $lastAlertTime{"$myDB:LATENCY"};
        }
        print EMAIL "  echo '    - data in the rs_ticket_history table indicated a latency of $latencyHold{$myDB} seconds which was above or equal to the specified threshold of $threshold seconds. $lastLatencyAlertLit. <BR>'\n";
      }
    }
    print EMAIL "  echo '<BR>Note that no further alerts will be produced for latency issues for at least $delay minutes <BR> <BR>'\n";
    print EMAIL '  echo \'<kbd><pre><font face="Courier New" size=2>' . "'\n";

    if ( ! open(TMPREP, "<$tmpRep") ) { die "Unable to open $tmpRep\n$!\n"; }
    else { # copy the report into the email .....
      while ( <TMPREP> ) {
         chomp $_;
         print EMAIL "  echo '$_'\n" ;
      }
      close TMPREP;
    }

    print EMAIL '  echo \'</font></pre></kbd>' . "'\n";

    print EMAIL ') | /usr/lib/sendmail ' . $email . "\n";

    close EMAIL;

    my $tmpResp = `chmod a+x $emailFile`;
       $tmpResp = `$emailFile`;

  }

  # adjust the times
  foreach my $x ( keys %currentAlerts ) {
    $lastAlertTime{$x} = $TS;
  }

 # save the times
  if ( ! open(LAST_ALERT, ">$lastAlertFile") ) {
    die "Unable to open $lastAlertFile\n$!\n";
  }
  else { # save the data
    foreach my $x ( keys %lastAlertTime ) {
      if ( $debugLevel > 0 ) { print "Saving key <$x> time <$lastAlertTime{$x}>\n"; }
      print LAST_ALERT "$x:$lastAlertTime{$x}\n";
    }

    close LAST_ALERT;

  }

}

sub raiseAlerts {
  # do something if the alert delay has been breached for each key
  # basically ensuring that alerts aren't raised too frequently

  foreach my $x ( keys %currentAlerts ) {
    my ($dbIn, $alertType) = split (':', $x);
    if ( defined( $OK_State{$alertType} ) ) { next; } # the alert type is an allowable state
    if ( defined( $excludeList{$dbIn} ) ) { next; } # database in the exclude list so dont raise alertG
    if ( $debugLevel > 0 ) { print "Checking $x time of $lastAlertTime{$x} against time $TS\n";}
    if ( ! defined( $lastAlertTime{$x} ) ) { # no alerts yet for this queue
      $lastAlertTime{$x} = $TS;
      return 1;
    }
    else { # check if the delay has occurred yet
      if (timeDiff($lastAlertTime{$x}, $TS, 'M') >= $delay ) { # we've waited long enough
        $lastAlertAlarmKey = $x;
        return 1;
      }
    }
  }
  return 0;
}

sub processTicketInfo {

  my $database = shift;

  # open the generated data for reporting on

  if ( ! open (REPIN , "<$tmpOut" ) ) { die "unable to open $tmpOut\n$!\n"; }

  # process the data

  $TS = getTimestamp();

  $databaseNotMonitored = 1;

  while ( <REPIN> ) {

    chomp $_;

    $currentSection = ''; # clear the section data as section is line by line

    if ( $_ =~ /Attempt to locate entry in sysdatabases for database \'.*\' by name failed/ ) { # database name supplied not found
      $cantConnect = 1;
      last; # leave now as any subsequent data will be for the master database
    }

    if ( $_ =~ /Database \'.*\' is currently offline/ ) { # database is currently offline (possibly because it has just been loaded)
      $offline = 1;
      last; # leave now as any subsequent data will be for the master database
    }

    if ( $_ =~ /Database \'.*\' is unavailable. It is undergoing LOAD DATABASE./ ) { # database name supplied not found
      $beingLoaded = 1;
      last; # leave now as any subsequent data will be for the master database
    }

    if ( $_ =~ /Requested server name not found./ ) { # Replication Target server is down
      $serverDown = 1;
      last; # leave now as any subsequent data will be for the master database
    }

    # figure out where you are ....

    if ( ($_ =~ / heartbeat /) || ($_ =~ / hpovlatchk /) || ($_ =~ / MSA_check /)  ) { $currentSection = 'data'; }

    if ( $debugLevel > 1 ) { print "INPUT ($currentSection) : $_\n"; }

    if ( $_ =~ /^ ---/ ) { next; } # skip the heading underline lines

    # process the sectional data that you are in

    if ( $currentSection eq 'data' ) { # process the command output
      $databaseNotMonitored = 0;
      processData($database);
    }
    else { # save the messages for later
      if ( ($_ =~ /\(1 row affected\)/) ||          # ignore line result count
           ($_ =~ /Msg 937/ ) ||                    # database being loaded
           ($_ =~ /Msg 950/ ) ||                    # database offline
           ($_ =~ /Server \'.*\', Line 1\:/ ) ||    #  generic server line
           (trim($_) eq '' ) ||                     # ignore blank lines
           ($_ =~ /cnt         h1         pdb/) ) { # ignore the heading line 
        next;
      }
      else {
        print OUTPUTDUMP "$_\n";
      }
    }

  } # end of while REPIN

  close REPIN;

  unlink $tmpOut;

  if ( $databaseNotMonitored ) { # database either not being monitored or cant get to ticket table
    checkHeader();
    my $exclLit = '';
    if ( defined($excludeList{$database}) ) { $exclLit = '(database excluded from alerts)' ; } 

    if ( $beingLoaded ) {
      $currentAlerts{"$database:DB_Being_Loaded"} = 1; # flag the fact that database is being loaded
      printf TMPREP "%-8s %10s %15s %19s %19s %7s %7s *** Database being loaded %31s\n", '0', 'GENERATED', $database, "$TS", "$TS", '0', '0', $exclLit ;
      printf "%-8s %10s %15s %19s %19s %7s %7s *** Database being loaded %31s\n", '0', 'GENERATED', $database, "$TS", "$TS", '0', '0', $exclLit ;
    }
    elsif ( $offline ) {
      $currentAlerts{"$database:DB_Offline"} = 1; # flag the fact that database is offline
      printf TMPREP "%-8s %10s %15s %19s %19s %7s %7s *** Database offline %31s\n", '0', 'GENERATED', $database, "$TS", "$TS", '0', '0', $exclLit ;
      printf "%-8s %10s %15s %19s %19s %7s %7s *** Database offline %31s\n", '0', 'GENERATED', $database, "$TS", "$TS", '0', '0', $exclLit ;
    }
    elsif ( $serverDown ) {
      $currentAlerts{"$database:Server_Down"} = 1; # flag the fact that something wrong with the server
      printf TMPREP "%-8s %10s %15s %19s %19s %7s %7s *** Connect to $server failed %31s\n", '0', 'GENERATED', $database, "$TS", "$TS", '0', '0', $exclLit ;
      printf "%-8s %10s %15s %19s %19s %7s %7s *** Connect to $server failed %31s\n", '0', 'GENERATED', $database, "$TS", "$TS", '0', '0', $exclLit ;
      if ( defined($excludeList{$database}) ) { $exitCode = 0; }
      else { $exitCode = 8; }
    }
    elsif ( $cantConnect ) {
      $currentAlerts{"$database:DB_Not_Found"} = 1; # flag the fact that the database has not been found
      printf TMPREP "%-8s %10s %15s %19s %19s %7s %7s *** Database not found %31s\n", '0', 'GENERATED', $database, "$TS", "$TS", '0', '0', $exclLit ;
      printf "%-8s %10s %15s %19s %19s %7s %7s *** Database not found %31s\n", '0', 'GENERATED', $database, "$TS", "$TS", '0', '0', $exclLit ;
      if ( defined($excludeList{$database}) ) { $exitCode = 0; }
      else { $exitCode = 8; }
    }
    else { # cant get to database (or there is no heartbeat information)
      $currentAlerts{"$database:No_Heartbeat_Data"} = 1; # flag the fact that something wrong with the latency
      printf TMPREP "%-8s %10s %15s %19s %19s %7s %7s *** Database not being monitored %31s\n", '0', 'GENERATED', $database, "$TS", "$TS", '0', '0', $exclLit ;
      printf "%-8s %10s %15s %19s %19s %7s %7s *** Database not being monitored %31s\n", '0', 'GENERATED', $database, "$TS", "$TS", '0', '0', $exclLit ;
    }
  }

} # end of processTicketInfo

sub processData {

  # process the information returned from the command

  # 420119 hpovlatchk master                         2017-03-03 10:25:09     2017-03-03 10:25:09

  my $dbIn = shift; # database being processed

  my ( $cnt, $header, $database, $dt_1, $tm_1, $dt_2, $tm_2) = split (" ");

  $lastRun{$database} = timeDiff("$dt_1 $tm_1", "$TS", 'S');
  my $latency = timeDiff("$dt_1 $tm_1", "$dt_2 $tm_2", 'S');
  my $calculatedLatency = $latency;

  # $lastRun is the number of seconds from the last heartbeat start to the current time
  if ( $lastRun{$database} > $heartbeatFreq ) { # we've been waiting longer than the heartbeat frequency so calculate it
    $calculatedLatency = timeDiff("$dt_1 $tm_1", "$TS", 'S') - $heartbeatFreq;
  }

  $latencyHold{$dbIn} = $latency;

  # now check the last time that a record was sent through ( fixed limit of 10 minutes )

  if ( $lastRun{$database} >= $waitThreshold ) { # a latency check hasn't been done in in the required time

    checkHeader();

    # $calculatedLatency = $calculatedLatency + $heartbeatFreq;

    if ( defined( $excludeList{$dbIn} ) ) { # database being excluded from alerting
      $exitCode = 0; 
      printf TMPREP "%-8s %10s %15s %19s %19s %7s %7s *** Last Latency check done too long ago ($waitThreshold) - but database being excluded from alerting\n", $cnt, $header, $database, $dt_1 . " " . $tm_1, $dt_2 . " " . $tm_2, $latency, $calculatedLatency ;
      printf "%-8s %10s %15s %19s %19s %7s %7s *** Last Latency check done too long ago ($waitThreshold) - but database being excluded from alerting\n", $cnt, $header, $database, $dt_1 . " " . $tm_1, $dt_2 . " " . $tm_2, $latency, $calculatedLatency ;
    }
    else { # not excluded
      $exitCode = 8;
      $currentAlerts{"$dbIn:Last_LATENCY_CHK"} = 1; # flag the fact that something wrong with when the last check was done

      printf TMPREP "%-8s %10s %15s %19s %19s %7s %7s *** Last Latency check done too long ago\n", $cnt, $header, $database, $dt_1 . " " . $tm_1, $dt_2 . " " . $tm_2, $latency, $calculatedLatency ;
      printf "%-8s %10s %15s %19s %19s %7s %7s *** Last Latency check done too long ago\n", $cnt, $header, $database, $dt_1 . " " . $tm_1, $dt_2 . " " . $tm_2, $latency, $calculatedLatency ;

      print TMPREP "Critical: The last latency check was done at $dt_1 $tm_1 (" . elapsedLit($lastRun{$database}) . " ago - threshold $waitThreshold)\n          [Last alert was at " . $lastAlertTime{"$database:Last_LATENCY_CHK"} . "]\n\n";
      print "Critical: The last latency check was done at $dt_1 $tm_1 (" . elapsedLit($lastRun{$database}) . " ago - threshold $waitThreshold)\n          [Last alert was at " . $lastAlertTime{"$database:Last_LATENCY_CHK"} . "]\n\n";
    }
  }
  elsif ($latency >= $threshold ) { # check on latency given that we have checked in the last 10 mins

    checkHeader();

    if ( defined( $excludeList{$dbIn} ) ) { # database being excluded from alerting
      $exitCode = 0; 
      printf TMPREP "%-8s %10s %15s %19s %19s %7s %7s *** Latency too long ($threshold) - but database being excluded from alerting\n", $cnt, $header, $database, $dt_1 . " " . $tm_1, $dt_2 . " " . $tm_2, $latency, $calculatedLatency ;
      printf "%-8s %10s %15s %19s %19s %7s %7s *** Latency too long ($threshold) - but database being excluded from alerting\n", $cnt, $header, $database, $dt_1 . " " . $tm_1, $dt_2 . " " . $tm_2, $latency, $calculatedLatency ;
    }
    else {
      $exitCode = 8;
      $currentAlerts{"$dbIn:LATENCY"} = 1; # flag the fact that something wrong with the latency

      printf TMPREP "%-8s %10s %15s %19s %19s %7s %7s *** Latency too long ($threshold)\n", $cnt, $header, $database, $dt_1 . " " . $tm_1, $dt_2 . " " . $tm_2, $latency, $calculatedLatency ;
      printf "%-8s %10s %15s %19s %19s %7s %7s *** Latency too long ($threshold)\n", $cnt, $header, $database, $dt_1 . " " . $tm_1, $dt_2 . " " . $tm_2, $latency, $calculatedLatency ;
    }
  }
  else { # all is good just put the details in the reportin case it is needed
    checkHeader();
    printf TMPREP "%-8s %10s %15s %19s %19s %7s %7s \n", $cnt, $header, $database, $dt_1 . " " . $tm_1, $dt_2 . " " . $tm_2, $latency, $calculatedLatency ;
    printf "%-8s %10s %15s %19s %19s %7s %7s \n", $cnt, $header, $database, $dt_1 . " " . $tm_1, $dt_2 . " " . $tm_2, $latency, $calculatedLatency ;
  }

} # end of processData

sub checkHeader {

  # ------------------------------------------------------------------------------
  # Check if a header has been printed and if not then print one
  # ------------------------------------------------------------------------------

  if ( $header ) {
    print  TMPREP "Snapshot time: $TS\n\n";
    printf TMPREP "%-8s %10s %15s %19s %19s %7s %7s \n", 'ID', 'Header', 'Database', 'Insert Time', 'Replicated Time', 'Latency', 'Cal Lat' ;
    printf TMPREP "%-8s %10s %15s %19s %19s %7s %7s\n", '--------', '----------', '---------------', '-------------------', '-------------------','-------','-------' ;
    print  "Snapshot time: $TS\n\n";
    printf "%-8s %10s %15s %19s %19s %7s %7s\n", 'ID', 'Header', 'Database', 'Insert Time', 'Replicated Time', 'Latency', 'Cal Lat' ;
    printf "%-8s %10s %15s %19s %19s %7s %7s\n", '--------', '----------', '---------------', '-------------------', '-------------------','-------','-------' ;

    $header = 0;

  }

}

sub getTimestamp {

  # ------------------------------------------------------------------------------
  # Get timestamp in format YYYY.MM.DD HH:MM:SS
  # ------------------------------------------------------------------------------

  my ($second, $minute, $hour, $dayOfMonth, $month, $yearOffset, $dayOfWeek, $dayOfYear, $daylightSavings) = localtime();
  my $year = 1900 + $yearOffset;
  $month = $month + 1;
  $hour = substr("0" . $hour, length($hour)-1,2);
  $minute = substr("0" . $minute, length($minute)-1,2);
  $second = substr("0" . $second, length($second)-1,2);
  $month = substr("0" . $month, length($month)-1,2);
  my $day = substr("0" . $dayOfMonth, length($dayOfMonth)-1,2);
  my $NowTS = "$year-$month-$day $hour:$minute:$second";

  return $NowTS;

}

sub elapsedLit {

  # ------------------------------------------------------------------------------
  # Given a number of seconds this routine will displaya literal in the format of
  # x days x mins x seconds
  # ------------------------------------------------------------------------------

  my $total_elapsed = shift; # elapsed in seconds
  my $ret = '';
  if ( $debugLevel > 0 ) { print "total_elapsed=$total_elapsed\n"; }

  my $elapsed_days = int($total_elapsed / (60.0 * 60 * 24));
  if ( ($elapsed_days > 0)  || ($ret ne '' )  ) { 
    if ( $elapsed_days == 1 ) {
      $ret .= "$elapsed_days day "; 
    }
    else {
      $ret .= "$elapsed_days days "; 
    }
  }
  $total_elapsed = $total_elapsed - ($elapsed_days * 60.0 * 60 * 24);
  if ( $debugLevel > 0 ) { print "elapsed_days=$elapsed_days, total_elapsed=$total_elapsed, ret=$ret\n"; }

  my $elapsed_Hrs = int($total_elapsed / (60.0 * 60));
  if ( ($elapsed_Hrs > 0)  || ($ret ne '' )  ) { 
    if ( $elapsed_Hrs == 1 ) {
      $ret .= "$elapsed_Hrs hour "; 
    }  
    else {
      $ret .= "$elapsed_Hrs hours "; 
    }
  }
  $total_elapsed = $total_elapsed - ($elapsed_Hrs * 60.0 * 60);
  if ( $debugLevel > 0 ) { print "elapsed_Hrs=$elapsed_Hrs, total_elapsed=$total_elapsed, ret=$ret\n"; }

  my $elapsed_Mins = int($total_elapsed / 60.0);
  if ( ($elapsed_Mins > 0)  || ($ret ne '' )  ) { 
    if ( $elapsed_Mins == 1 ) {
      $ret .= "$elapsed_Mins min "; 
    }
    else {
      $ret .= "$elapsed_Mins mins "; 
    }
  }
  $total_elapsed = $total_elapsed - ($elapsed_Mins * 60.0 );
  if ( $debugLevel > 0 ) { print "elapsed_Mins=$elapsed_Mins, total_elapsed=$total_elapsed, ret=$ret\n"; }

  my $conj = '';
  if ( $ret ne '' ) { # greater than 59 seconds
    $conj = 'and ';
  }

  if ( $total_elapsed == 1 ) {
    $ret .= "$conj$total_elapsed second";
  }
  else {
    $ret .= "$conj$total_elapsed seconds";
  }

  return $ret;
}

sub getTicketInfo {

  # ------------------------------------------------------------------------------
  # get the data from rs_ticket_history table and place the information in
  # the $tmpOut file
  # ------------------------------------------------------------------------------

  my $database = shift;

  if ( $debugLevel > 0 ) {print "CMD: isql -w 3000 -D $database -S $server -U sybmaint -P <sybmaint PWD> -i $tmpIn -o $tmpOut\n";}
  my $tmp = `isql -w 3000 -D $database -S $server -U sybmaint -P $tmpPWD -i $tmpIn -o $tmpOut`;

  if ( $debugLevel > 0 ) {
    print ">>>>> PWDLoc: $PWDLoc\n";
    print ">>>>> tmpOut: $tmpOut\n";
    print ">>>>> tmp: $tmp\n";
  }
}

sub waitSeconds {

  # ------------------------------------------------------------------------------
  # wait the specified number of seconds
  # ------------------------------------------------------------------------------

  my $x;

  # wait a while
  if ( $number > 0 ) {
    if ( $OS eq "Windows" ) {
      $x = `PING 192.168.18.9 -n 1 -w $waitMS`;
    }
    else {
      $x = `/usr/sbin/ping 192.168.18.9 $wait`;
    }
  }

}

sub loadExcludeList {

  # -------------------------------------------------------------------------------
  # Load any databases that are to be excluded from raising alerts
  # -------------------------------------------------------------------------------

  %excludeList = ();      # initialise array
  if ( open(EXCL, "$exclDBFile") ) {
    while (<EXCL>) {
      chomp $_;
      if ( $_ !~ /^#/ ) {       # not comment
        if ( trim($_) ne '' ) { # not empty
          $excludeList{"$_"} = 1;
        }
      }
    }

    close EXCL;

  }
  else {
    if ( $debugLevel > 0 ) { print "***** No excluded databases loaded\n"; }
  }

}

sub processDatabase {

  # -------------------------------------------------------------------------------
  # check the latency for an individual database
  # -------------------------------------------------------------------------------

  my $database = shift; # database needs to be passed

  getTicketInfo($database);
  processTicketInfo($database); # this will prcess the data returned in $tmpOut

}

# ----------------------------------------------------
# -- Start of Parameter Section
# ----------------------------------------------------

# Initialise vars for getOpt ....

#my $test = 1;

while ( getOpt(":?hsxvg:S:n:w:W:T:t:d:e:D:f:") ) {
 if (($getOpt_optName eq "h") || ($getOpt_optName eq "?") )  {
   usage ("");
   exit;
 }
 elsif (($getOpt_optName eq "s"))  {
   $silent = "Yes";
 }
 elsif (($getOpt_optName eq "x"))  {
   if ( $silent ne "Yes") {
     print "Emails wont be sent - screen output only\n";
   }
   $screenOnly = 1;
 }
 elsif ($getOpt_optName eq "d")  {
   $delay = "";
   ($delay) = ($getOpt_optValue =~ /(\d*)/);
   if ($delay eq "") {
      usage ("Value supplied for the delay parameter (-d) is not numeric");
      exit;
   }
   if ( $silent ne "Yes") {
     print "There will be a $delay minute delay between alerts for databases\n";
   }
 }
 elsif ($getOpt_optName eq "t")  {
   $threshold = "";
   ($threshold) = ($getOpt_optValue =~ /(\d*)/);
   if ($threshold eq "") {
      usage ("Value supplied for the threshold parameter (-t) is not numeric");
      exit;
   }
   if ( $silent ne "Yes") {
     print "Any latency greater than $threshold seconds will be detected\n";
   }
 }
 elsif ($getOpt_optName eq "T")  {
   $waitThreshold = "";
   ($waitThreshold) = ($getOpt_optValue =~ /(\d*)/);
   if ($waitThreshold eq "") {
      usage ("Value supplied for the heartbeat threshold parameter (-T) is not numeric");
      exit;
   }
   if ( $silent ne "Yes") {
     print "Any heartbeats greater than $waitThreshold seconds apart will be detected\n";
   }
 }
 elsif ($getOpt_optName eq "w")  {
   $wait = "";
   ($wait) = ($getOpt_optValue =~ /(\d*)/);
   if ($wait eq "") {
      usage ("Value supplied for the wait parameter (-w) is not numeric");
      exit;
   }
   if ( $silent ne "Yes") {
     print "Monitor will wait $wait seconds before iteration\n";
   }
   $waitMS = $wait * 1000;
 }
 elsif ($getOpt_optName eq "W")  {
   $heartbeatFreq = "";
   ($heartbeatFreq) = ($getOpt_optValue =~ /(\d*)/);
   if ($heartbeatFreq eq "") {
      usage ("Value supplied for the hearbeat frequency parameter (-W) is not numeric");
      exit;
   }
   if ( $silent ne "Yes") {
     print "$heartbeatFreq will be used as the heartbeat frequency\n";
   }
 }
 elsif ($getOpt_optName eq "n")  {
   $number = "";
   ($number) = ($getOpt_optValue =~ /(\d*)/);
   if ($number eq "") {
      usage ("Value supplied for number parameter (-n) is not numeric");
      exit;
   }
   if ( $silent ne "Yes") {
     print "Monitor will iterate $number times\n";
   }
 }
 elsif (($getOpt_optName eq "g"))  {
   $fileSuff = "_$getOpt_optValue";
   if ( $silent ne "Yes") {
     print "Files will be suffixed with $fileSuff\n";
   }
 }
 elsif (($getOpt_optName eq "e"))  {
   $email = $getOpt_optValue;
   if ( $silent ne "Yes") {
     print "Email alerts will be sent to $email\n";
   }
 }
 elsif (($getOpt_optName eq "D"))  {
   $databases = $getOpt_optValue;
   if ( $silent ne "Yes") {
     print "Databases loaded from the command line are: $databases\n";
   }
 }
 elsif (($getOpt_optName eq "f"))  {
   $databaseFile = $getOpt_optValue;
   if ( $silent ne "Yes") {
     print "Databases to be processed will be loaded from $databaseFile\n";
   }
 }
 elsif (($getOpt_optName eq "v"))  {
   $debugLevel++;
   if ( $silent ne "Yes") {
     print "Debug Level set to $debugLevel\n";
   }
 }
 elsif (($getOpt_optName eq "S"))  {
   $server = $getOpt_optValue;;
   if ( $silent ne "Yes") {
     print "Server $server will be used\n";
   }
 }
 else { # handle other entered values ....
   usage ("Parameter $getOpt_optName : This parameter is unknown");
   exit;
 }
}

# ----------------------------------------------------
# -- End of Parameter Section
# ----------------------------------------------------

chomp $machine;
my ($second, $minute, $hour, $dayOfMonth, $month, $yearOffset, $dayOfWeek, $dayOfYear, $daylightSavings) = localtime();
my $year = 1900 + $yearOffset;
$month = $month + 1;
$hour = substr("0" . $hour, length($hour)-1,2);
$minute = substr("0" . $minute, length($minute)-1,2);
$second = substr("0" . $second, length($second)-1,2);
$month = substr("0" . $month, length($month)-1,2);
my $day = substr("0" . $dayOfMonth, length($dayOfMonth)-1,2);
my $NowTS = "$year.$month.$day $hour:$minute:$second";
$user = getpwuid($<);

%monthNumber = ( 'Jan' =>  '01', 'Feb' =>  '02', 'Mar' =>  '03', 'Apr' =>  '04', 'May' =>  '05', 'Jun' =>  '06',
                    'Jul' =>  '07', 'Aug' =>  '08', 'Sep' =>  '09', 'Oct' =>  '10', 'Nov' =>  '11', 'Dec' =>  '12',
                    'January' =>  '01', 'February' =>  '02', 'March' =>  '03', 'April' =>  '04', 'May' =>  '05', 'June' =>  '06',
                    'July' =>  '07', 'August' =>  '08', 'September' =>  '09', 'October' =>  '10', 'November' =>  '11', 'December' =>  '12' );

# Adjust all file names to include the supplied suffix (if one was supplied)

if ( $fileSuff ne '' ) {
  if ( $databaseFile ne '' ) {
    $databaseFile .= $fileSuff;
  }
  $emailFile = '/var/tmp/checkLatency_email' . ${fileSuff} . '.ksh';
  $lastAlertFile = 'last_alert' . $fileSuff . '.txt';
  $exclDBFile = 'monitoringList_exclusions' . $fileSuff;
  $checkLatMsgFile = 'checkLatency_msgs' . $fileSuff . '.txt';
  $reportingServerFile = "latency/checkLatency_${server}.out" . $fileSuff;
}
else { # allocate the file names with out the suffix
  $emailFile = '/var/tmp/checkLatency_email.ksh';
  $lastAlertFile = 'last_alert.txt';
  $exclDBFile = 'monitoringList_exclusions';
  $checkLatMsgFile = 'checkLatency_msgs.txt';
  $reportingServerFile = "latency/checkLatency_${server}.out";
}

# read in databases from the file specified (if one was specified)
if ( $databaseFile ne '' ) {
  if ( ! open(DB, "<$databaseFile") ) { die "Unable to open $databaseFile\n$!\n"; }
  while ( <DB> ) {
    chomp $_;
    if ( (trim($_) ne '' ) && ( $_ !~ /^\#/ ) ) {  # not a blank line or a comment
      if ( $databases eq '' ) {
        $databases = trim($_);
      }
      else {
        $databases .= "," . trim($_);
      }
    }
  }
  close DB;
}
if ( $databases eq '' ) { $databases = 'master'; }

# Open a file to write commound output strings to (to aid in problem determination)

if ( ! open(OUTPUTDUMP, ">>$checkLatMsgFile") ) { # unable to open the file
  die "Unable to open $checkLatMsgFile\n$?" 
}

# load last alert times if available

%lastAlertTime = ();      # initialise array
if ( open(LAST_ALERT, "$lastAlertFile") ) {
  while (<LAST_ALERT>) {
    chomp $_;
    my @bit = split (":", $_,3);
    if ( $debugLevel > 0 ) { print "Last $bit[1] alert for $bit[0] has been set to $bit[2]\n"; }
    $lastAlertTime{"$bit[0]:$bit[1]"} = $bit[2];
  }

  close LAST_ALERT;

}
else {
  if ( $debugLevel > 0 ) { print "***** No last alerts loaded (RC: $?)\n"; }
}

# START OF THE PROGRAM

# loop as specified  .... $number times with a $wait seconds wait in between

while ( $number > 0 ) {

  %currentAlerts = ();
  %latencyHold = ();
  $exitCode = 0;
  $header = 1;

  loadExcludeList();  # reload the exdclude list each time just in case it has changed

  # create temporary output file

  if ( ! open(TMPREP, ">$tmpRep") ) { die "Unable to create temporary reporting file $tmpRep\n$!\n"; }

  my @dblist = split(',', $databases);
  foreach  my $database (@dblist) {
    $cantConnect = 0;
    $beingLoaded = 0;
    $serverDown = 0;
    processDatabase($database);
  }

  close TMPREP;

  if ( $exitCode > 4 ) { # something bad happened
    if ( raiseAlerts() ) { # check to see that we haven't sent an email about this recently
      sendEmail();
    }
  }

  if ( $fileSuff ne '' ) {
  }
  else { # allocate the file names with out the suffix
  }

  my $ret = `scp $tmpRep sybase\@192.168.1.1:$reportingServerFile`;
  unlink $tmpRep; # delete the generated report file

  $number--;
  if ( $number > 0 ) { waitSeconds(); }


}

close OUTPUTDUMP;

exit $exitCode;

