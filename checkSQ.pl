#!/usr/bin/perl
# --------------------------------------------------------------------
# checkSQ.pl
#
# $Id: checkSQ.pl,v 1.23 2020/11/25 03:55:49 db2admin Exp db2admin $
#
# Description:
# Script to check the stable queue
#
# Usage:
#   checkSQ.pl -f <filename>
#
# $Name:  $
#
# ChangeLog:
# $Log: checkSQ.pl,v $
# Revision 1.23  2020/11/25 03:55:49  db2admin
# put in code to avoid div by zero errors when space allocated is 0
#
# Revision 1.22  2019/08/23 05:16:18  db2admin
# make sure waiting on non-existent IP address
#
# Revision 1.21  2019/01/25 04:08:18  db2admin
# correct bug with previous change where replace done incorrectly
#
# Revision 1.20  2019/01/25 03:12:40  db2admin
# adjust commonFunctions.pm parameter importing to match module definition
#
# Revision 1.19  2018/04/13 05:43:42  db2admin
# removeoutput line count from report subroutine
#
# Revision 1.18  2018/04/13 01:57:03  db2admin
# add in page size parameter (defaults to 20) - establishes when to reprint headers
#
# Revision 1.17  2018/02/14 04:31:45  db2admin
# ensure minutes are integer
#
# Revision 1.16  2018/02/13 23:06:49  db2admin
# only try and predict queue clear time when the queues are decreasing
#
# Revision 1.15  2018/02/13 22:57:13  db2admin
# add in timing predictions for the queues to empty
#
# Revision 1.14  2018/02/13 22:23:10  db2admin
# add in option to calculate average replication rate from the first collected figures rather than between snapshots
# and allow rate information to be displayed for down to 30 seconds
#
# Revision 1.13  2018/01/16 05:18:28  db2admin
# add in info only message
#
# Revision 1.12  2017/12/12 01:26:42  db2admin
# correct report formatting
#
# Revision 1.11  2017/12/12 00:12:43  db2admin
# add in john to default email address (until we start using the standard Sybase emailing address)
# just for testing
#
# Revision 1.10  2017/03/01 11:55:28  db2admin
# Restructure the way that the queue display is printed to simplify maintenance
#
# Revision 1.9  2017/03/01 04:37:13  db2admin
# add in code to calculate Mb change in queue size per poll
#
# Revision 1.8  2017/03/01 01:14:28  db2admin
# correct usage description
#
# Revision 1.7  2017/03/01 01:03:44  db2admin
# alter usage information
#
# Revision 1.6  2017/03/01 00:20:07  db2admin
# ensure % is printed
#
# Revision 1.5  2017/03/01 00:10:02  db2admin
# display physical disk size usage as necessary
#
# Revision 1.4  2017/02/28 23:32:51  db2admin
# Ensure heading are displayed on change of report
# correct display of who_is_down data
#
# Revision 1.3  2017/02/28 22:32:51  db2admin
# add in reporting for DISK_SIZE and WHO_IS_DOWN
# both reports are also capable of raising alerts
#
# Revision 1.2  2017/02/28 05:08:48  db2admin
# Add in a number of changes:
# -d delay period for alerting
# -e email address to send alerts to
# -x only output to the screen - no emails
# NOTE: exceeding threshold (-t) is currently the only monitor that will alert
#
# Revision 1.1  2017/02/27 11:42:36  db2admin
# Initial revision
#
#
# --------------------------------------------------------------------

use strict;

my $debugLevel = 0;
my %monthNumber;

my $ID = '$Id: checkSQ.pl,v 1.23 2020/11/25 03:55:49 db2admin Exp db2admin $';
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
my %queueSize = ();
my $exitCode = 0;
my %lastAlertTime = ();
my %currentAlerts = ();
my $TS;
my $warn = 'CRITICAL ';
my $minsDiff = 0;
my $lastAlertAlarmKey = '';
my $totalPhysicalSegs = 0;
my $totalUsedSegs = 0;
my $displayDiskSize = 0;
my $warningsPrinted = 0;
my %lastTime = ();
my %lastSize = ();
my $outputLineCount = 0;   # count of the number of lines output since last heading

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

use commonFunctions qw(getOpt myDate trim $getOpt_optName $getOpt_optValue @myDate_ReturnDesc $cF_debugLevel timeDiff displayMinutes timeAdj);

# Subroutines and functions ......

sub by_key {
  $a cmp $b ;
}

sub usage {
  if ( $#_ > -1 ) {
    if ( trim("$_[0]") ne "" ) {
      print "\n$_[0]\n\n";
    }
  }

  print "Usage: $0 -?hs [-f <file>] [-v[v]] [-S <server name>] [-a] [-n <number>] [-w <number>] [-t <number>] [-P <page size>]
                        [-d <number>] [-e <email address>] [-x] [-A] [-p]

       Script to check the existence of a Netbackup backup of a file

       Version $Version Last Changed on $Changed (UTC)

       -h or -?        : This help message
       -s              : Silent mode (no parameter information will be displayed)
       -f              : file name of file holding the report to process
       -S              : server to connect to 
       -a              : display all stable devices (by default only those with data are displayed)
       -n              : number of iterations (default 1)
       -w              : wait between iterations (in seconds, default 60)
       -t              : display entries where queue is greater than this number of Mb 
                         (when set ONLY queues greater than this value will be displayed)
       -d              : alerting delay in minutes to occur for delays between alerts for a database (defaults to 240)
       -p              : display completion prediction times
       -P              : number of lines to consider a page of information (before headers are reprinted)
       -e              : email address to send alerts to (defaults to ????)
       -x              : dont send emails just display to screen
                         (this will also prevent the last alert timesatmps from being updated)
       -A              : rate change figures will be averaged across the whole of the run and not from snapshot to snapshot
       -v              : debug level

  Exit codes: 0 - all queues are empty
              2 - some queues are not empty
              4 - some queues have exceeded the Mb threshold

\n";

}

my $silent = "No";
my $inFile = '';
my $server = '';
my $allDevices = 0;
my $number = 1;
my $wait = 60;
my $waitMS = 60 * 1000;
my $threshold = 1;
my $email = 'webmaster@KAGJCM.com.au,john.seguel@KAGJCM.com.au';
my $delay = 240;
my $screenOnly = 0;
my $rateaverage = 0;
my $displayPrediction = 0;
my $pageSize = 20;

# ----------------------------------------------------
# -- Start of Parameter Section
# ----------------------------------------------------

# Initialise vars for getOpt ....

while ( getOpt(":?hsxaApf:vS:n:w:t:d:e:P:") ) {
 if (($getOpt_optName eq "h") || ($getOpt_optName eq "?") )  {
   usage ("");
   exit;
 }
 elsif (($getOpt_optName eq "s"))  {
   $silent = "Yes";
 }
 elsif (($getOpt_optName eq "f"))  {
   if ( $silent ne "Yes") {
     print "Backup file to be scanned is $getOpt_optValue\n";
   }
   $inFile = $getOpt_optValue;
 }
 elsif (($getOpt_optName eq "p"))  {
   if ( $silent ne "Yes") {
     print "Completion predictions will be displayed\n";
   }
   $displayPrediction = 1;
 }
 elsif (($getOpt_optName eq "A"))  {
   if ( $silent ne "Yes") {
     print "Rate changes will be averaged across the whole of the run\n";
   }
   $rateaverage = 1;
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
 elsif ($getOpt_optName eq "P")  {
   $pageSize = "";
   ($pageSize) = ($getOpt_optValue =~ /(\d*)/);
   if ($pageSize eq "") {
      usage ("Value supplied for the pageSize parameter (-P) is not numeric");
      exit;
   }
   if ( $silent ne "Yes") {
     print "$pageSize lines will be considered a page and headers will then be reprinted\n";
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
     print "Queues that are greater than $threshold Mb in size will be detected\n";
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
 elsif (($getOpt_optName eq "a"))  {
   $allDevices = 1;
   if ( $silent ne "Yes") {
     print "All stable devices will be listed\n";
   }
 }
 elsif (($getOpt_optName eq "e"))  {
   $email = $getOpt_optValue;;
   if ( $silent ne "Yes") {
     print "Email alerts will be sent to $email\n";
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

my $PWDLoc = $ENV{'DBA_SYBMAINT_PWD_FILE'};
my $tmpPWD = `cat $PWDLoc`;
chomp $tmpPWD;

%monthNumber = ( 'Jan' =>  '01', 'Feb' =>  '02', 'Mar' =>  '03', 'Apr' =>  '04', 'May' =>  '05', 'Jun' =>  '06',
                    'Jul' =>  '07', 'Aug' =>  '08', 'Sep' =>  '09', 'Oct' =>  '10', 'Nov' =>  '11', 'Dec' =>  '12',
                    'January' =>  '01', 'February' =>  '02', 'March' =>  '03', 'April' =>  '04', 'May' =>  '05', 'June' =>  '06',
                    'July' =>  '07', 'August' =>  '08', 'September' =>  '09', 'October' =>  '10', 'November' =>  '11', 'December' =>  '12' );

# create the data to report on

my $tmpIn = "$scriptDir/checkSQ.sql";
my $tmpRep = "/tmp/checkSQ_$$.rep";
my $tmpOut = "/tmp/checkSQ_$$.out";
my $tmpOut2 = "/tmp/checkSQ2_$$.out";

my $currentSection = '';
my $RSSD_server = '';
my $RSSD_database = '';

my $queueHeader = 1;
my $diskHeaderRep = 1;
my $diskHeader = 1;
my $downHeader = 1;
my $healthHeader = 1;
my ($Info_ID_Type,$Info_Q_identifier,$Duplicates,$Writes,$Reads,$Bytes,$B_Writes,$B_Filled,$B_Reads,$B_Cache,$Save_Int_Seg,$First_Seg_Block,$Last_Seg_Block,$Next_Read,$Readers,$Truncs);

sub getQueueInfo {

# get the 'admin who, sqm' etc. info from the rep server

  if ( $inFile  eq '' ) { 

    if ( $debugLevel > 0 ) {print "CMD: isql -w 3000 -S $server -U sybmaint -P <sybmaint PWD> -i $tmpIn -o $tmpOut\n";}
    my $tmp = `isql -w 3000 -S $server -U sybmaint -P $tmpPWD -i $tmpIn -o $tmpOut`;

    if ( $debugLevel > 0 ) { 
      print ">>>>> PWDLoc: $PWDLoc\n";
      print ">>>>> tmpOut: $tmpOut\n";
      print ">>>>> tmp: $tmp\n";
    }
  }
}

sub waitSeconds {

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

# load last alert times if available

%lastAlertTime = ();      # initialise array
if ( open(LAST_ALERT, "last_alert.txt") ) {
  while (<LAST_ALERT>) {
    chomp $_;
    my @bit = split (":", $_,2);
    if ( $debugLevel > 0 ) { print "Last alert for $bit[0] has been set to $bit[1]\n"; }
    $lastAlertTime{$bit[0]} = $bit[1];
  }

  close LAST_ALERT;

}
else {
  if ( $debugLevel > 0 ) { print "***** No last alerts loaded (RC: $?)\n"; }
}

# loop as specified  .... $number times with a $wait seconds wait in between

while ( $number > 0 ) {

  # create temporary output file 

  if ( ! open(TMPREP, ">$tmpRep") ) { die "Unable to create temporary reporting file $tmpRep\n$!\n"; } 

  %currentAlerts = ();
  $exitCode = 0;
  $totalPhysicalSegs = 0;
  $totalUsedSegs = 0;

  getQueueInfo();
  $warningsPrinted = 0;
  processQueueInfo();

  close TMPREP;

  if ( $exitCode > 4 ) { # something bad happened
    if ( raiseAlerts() ) { # check to see that we haven't sent an email about this recently
      sendEmail();
    }
  }

  unlink $tmpRep; # delete the generated report file

  $number--; 
  if ( $number > 0 ) { waitSeconds(); }


}

sub sendEmail {

  # send the email alerts, reset the lastAlertTimes and then save them

  if ( $screenOnly ) { return; } # the -x option has been set so no emails

  if ( $debugLevel > 0 ) { print "**** Sending the email \n"; }

  if ( ! open(EMAIL, ">/tmp/checkSQ_email.ksh") ) {
    die "Unable to open /tmp/checkSQ_email.ksh\n$!\n";
  }
  else { # create the email .....
    # create the commands to send the email

    print EMAIL "#!/bin/bash\n\n"; 
    print EMAIL '(' . "\n";
    print EMAIL "  echo 'To:$email'\n";
    print EMAIL '  echo \'From:do-not-reply@KAGJCM.com.au\'' . "\n";
    print EMAIL "  echo 'Subject: ${warn} " . substr($TS,0,14) . ": $server - Stable Queue Issues'\n";

    print EMAIL "  echo 'MIME-Version: 1.0'\n";
    print EMAIL '  echo \'Content-Type: multipart/mixed; boundary="-q1w2e3r4t5"' . "'\n";

    print EMAIL '  echo \'---q1w2e3r4t5' . "'\n";
    print EMAIL '  echo \'Content-type: text/html' . "'\n";
    print EMAIL '  echo \'<html>' . "'\n";
    print EMAIL '  echo \'<head><title>Stable Queue Issues</title></head>' . "'\n";
    print EMAIL '  echo \'<body style="font-family:arial">' . "'\n";
    print EMAIL '  echo \'<h4><u>' . "Issues Identified on the Stable Queue on $server at $TS" . '</u></h4>' . "'\n";
    print EMAIL "  echo 'The following report was produced on $server at $TS <BR>'\n";
    print EMAIL "  echo 'The report was produced because $minsDiff minutes had elapsed since the last alert for $lastAlertAlarmKey at $lastAlertTime{$lastAlertAlarmKey} <BR> <BR>'\n";
    print EMAIL "  echo 'Note that no further alerts will be produced for any of the listed queues for at least $delay minutes <BR> <BR>'\n";
    print EMAIL '  echo \'<kbd><pre><font face="Courier New" size=2>' . "'\n";

    if ( ! open(TMPREP, "<$tmpRep") ) { die "Unable to open /tmp/checkSQ_email.ksh\n$!\n"; }
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

    my $tmpResp = `chmod a+x /tmp/checkSQ_email.ksh`;
    my $tmpResp = `/tmp/checkSQ_email.ksh`;

  }

  # adjust the times
  foreach my $x ( keys %currentAlerts ) {
    $lastAlertTime{$x} = $TS;
  }

 # save the times
  if ( ! open(LAST_ALERT, ">last_alert.txt") ) {
    die "Unable to open last_alert.txt\n$!\n";
  }
  else { # save the data
    foreach my $x ( keys %lastAlertTime ) {
      print LAST_ALERT "$x:$lastAlertTime{$x}\n";
    }

    close LAST_ALERT;

  }

}

sub raiseAlerts {
  # do something if the alert delay has been breached for each key
  # basically ensuring that alerts aren't raised too frequently

  foreach my $x ( keys %currentAlerts ) {
    if ( $debugLevel > 0 ) { print "Checking $x time of $lastAlertTime{$x} against time $TS\n";}
    if ( ! defined( $lastAlertTime{$x} ) ) { # no alerts yet for this queue
      $lastAlertTime{$x} = $TS;
      return 1;
    }
    else { # check if the delay has occurred yet
      if (timeDiff($lastAlertTime{$x}, $TS) >= $delay ) { # we've waited long enough
        $lastAlertAlarmKey = $x;
        return 1;
      }
    }
  } 
  return 0;
}

sub processQueueInfo {

  # open the generated data for reporting on

  if ( $inFile  eq '' ) {
    if ( ! open (REPIN , "<$tmpOut" ) ) { die "unable to open $tmpOut\n$!\n"; }
  }
  else {
    if ( $inFile eq 'STDIN' ) {
      if ( ! open (REPIN , "-" ) ) { die "unable to open STDIN\n$!\n"; }
    }
    else {
      if ( ! open (REPIN , "<$inFile" ) ) { die "unable to open $inFile\n$!\n"; }
    }
  }

  # process the data

  $TS = getTimestamp();
  $displayDiskSize = 0;

  while ( <REPIN> ) {

    chomp $_;

    # figure out where you are ....

    if ( $_ =~ /^ Spid State/ ) { $currentSection = 'queue'; next; }
    elsif ( $_ =~ /^ Spid Name/ ) { $currentSection = 'who_is_down' ; next; }
    elsif ( $_ =~ /^ Mode/ ) { $currentSection = 'health' ; next; }
    elsif ( $_ =~ /^ Partition/ ) { $currentSection = 'disk' ; next; }

    if ( $debugLevel > 1 ) { print "INPUT ($currentSection) : $_\n"; }

    if ( $_ =~ /^ ---/ ) { next; } # skip the heading underline lines

    # process the sectional data that you are in

    if ( $currentSection eq 'queue' ) { # process the admin who, sqm command output
      processQueueSizeData();
    }
    elsif ( $currentSection eq 'who_is_down' ) { # process the admin who_is_down output
      processDownData();
    }
    elsif ( $currentSection eq 'health' ) { # process the admin health output
    }
    elsif ( $currentSection eq 'disk' ) { # process the admin disk_size output
      processDiskData();
    }

  } # end of while REPIN

  close REPIN;

  if ( $displayDiskSize ) { 
    # display current Queue Disk usage
    my $usedPCT ;

    if ( $totalPhysicalSegs == 0 ) { 
      $usedPCT = 0;
    }
    else {
      $usedPCT = ($totalUsedSegs * 100.0) / $totalPhysicalSegs;  
    }
    printf TMPREP "INFO: Queue Disk Space - Used Segments: %10s Total Segments %10s (%6.2f %% used)\n", $totalPhysicalSegs, $totalUsedSegs, $usedPCT;  
    printf "INFO: Queue Disk Space - Used Segments: %10s Total Segments: %10s (%6.2f %% used)\n", $totalUsedSegs, $totalPhysicalSegs, $usedPCT;
    $outputLineCount++;
    $warningsPrinted = 1;
  }

  # check total physical queue space
  if ( $totalPhysicalSegs > 0 ) { 
    if ( (($totalUsedSegs * 100.0) / $totalPhysicalSegs) > 90 ) { # more than 90% of the physical queue has been filled
      my $usedPCT = ($totalUsedSegs * 100.0) / $totalPhysicalSegs;
      $exitCode = 8; # set the alarm
      $currentAlerts{"QUEUEDISK|X"} = 1; # flag the fact that something wrong with this one [X indicates that it is a queue down]

      printf TMPREP "/n/nCRITICAL: Queue Disk Space - Used Segments: %10s Total Segments: %10s (%6.2f %% used)\n", $totalUsedSegs, $totalPhysicalSegs, $usedPCT;
      printf "/n/nCRITICAL: Queue Disk Space - Used Segments: %10s Total Segments: %10s (%6.2f %% used)\n", $totalUsedSegs, $totalPhysicalSegs, $usedPCT;
      $outputLineCount+=3;
      $warningsPrinted = 1;
    }
  }

  unlink $tmpOut;
  unlink $tmpOut2;
  
  if ( ! $warningsPrinted ) { # nothing printed so just say hello
    print TMPREP "$TS All OK\n";
    print "$TS All OK\n";
  }

} # end of processQueueInfo

sub processDiskData {

  # process the information returned from the 'admin disk_size' command
  # note this data is onle displayed when the -a option is selected
  # but $totalPhysicalSegs is used in other calculations

  # Partition                                                                                                                                                                                                                                                       Logical                         Part.Id     Total Segs  Used Segs   State

  # /prj/sybase/devices/dbdat0/sybrst15/queue29.dat                                                                                                                                                                                                                 queue29                                 129        2048           1 ON-LINE//

  my ( $file, $queue, $PartID, $totalSegs, $usedSegs, $State) = split(" ");

  $totalPhysicalSegs += $totalSegs;
  $totalUsedSegs += $usedSegs;

  if ( ($State !~ /ON-LINE/) || ( $usedSegs > 0 ) ) {
    checkDiskHeaderRep();
    printf TMPREP "%-19s %-10s %-6s %10s %10s %15s %-60s\n", $TS, $queue, $PartID, $totalSegs, $usedSegs, $State, $file;
  }
  if ( $allDevices ) {
    checkDiskHeader();
    printf "%-19s %-10s %-6s %10s %10s %15s %-60s\n", $TS, $queue, $PartID, $totalSegs, $usedSegs, $State, $file;
    $outputLineCount++;
    $warningsPrinted = 1;
    $displayDiskSize = 1;
  }

}

sub processDownData {
  
  # process the information returned from the 'admin who_is_down' command

  # Spid Name       State                Info
  # ---- ---------- -------------------- ----------------------------------------
  #      DSI EXEC   Suspended            1224(1) sybdbt16.iMedWORK
  #      DSI        Suspended            1224 sybdbt16.iMedWORK

  $_ .= '                                                                                      ';
  
  my $SPID = trim(substr($_,1,4));
  my $Name = trim(substr($_,6,10));
  my $State = trim(substr($_,17,20));
  my $Info = trim(substr($_,38));
  
  my @bit = split (" ", $Info); # bit[1] is the logical connection

  checkDownHeader();

  $exitCode = 8; # set the alarm
  $currentAlerts{"$bit[1]|X"} = 1; # flag the fact that something wrong with this one [X indicates that it is a queue down]

  printf TMPREP "%-19s %4s %-10s %-20s %-40s %-20s\n", $TS, $SPID, $Name, $State, $bit[1], $bit[0];
  printf "%-19s %4s %-10s %-20s %-40s %-20s\n", $TS, $SPID, $Name, $State, $bit[1], $bit[0];
  $outputLineCount++;
  $displayDiskSize = 1;
  $warningsPrinted = 1;
}

sub processQueueSizeData {

  my $sizeChange = '';
  my $changePerSec = '';
  my $predict = '';
  my $minstogo = 0;

  # process the data returned by the 'admin who, sqm' command

  my ($Spid,$State) = split (" ", substr($_,0,27),2) ;
  ($Info_ID_Type,$Info_Q_identifier,$Duplicates,$Writes,$Reads,$Bytes,$B_Writes,$B_Filled,$B_Reads,$B_Cache,$Save_Int_Seg,$First_Seg_Block,$Last_Seg_Block,$Next_Read,$Readers,$Truncs) = split (" ", substr($_,27));

  if ( $debugLevel > 2) { print "$Spid ~ $State ~ $Info_ID_Type ~ $Info_Q_identifier ~ $Duplicates ~ $Writes ~ $Reads ~ $Bytes ~ $B_Writes ~ $B_Filled ~ $B_Reads ~ $B_Cache ~ $Save_Int_Seg ~ $First_Seg_Block ~ $Last_Seg_Block ~ $Next_Read ~ $Readers ~ $Truncs\n"; }

  # if we have the database entry for the replication server database then get the sizes ....

  if ($Info_Q_identifier =~ /RSSD/ ) { # check if this is the database connection
    ($RSSD_server, $RSSD_database) = split('\.',$Info_Q_identifier);
    getQueueSize($RSSD_server,$RSSD_database);
  }

  my @bit = split (':',$Info_ID_Type);
  my $direction = "Inbound";
  if ( $bit[1] == 0 ) { $direction = "Outbound"; }
  my $dr = substr($direction,0,1);

  my $disState = "(" . trim($State) . ")";

  my $qs = '';
  if ( defined($queueSize{"$Info_Q_identifier|$dr"}) ) { $qs = $queueSize{"$Info_Q_identifier|$dr"}; }

  if ( $debugLevel > 0 ) { print "Queue size for $Info_Q_identifier|$dr is $qs\n"; }

  my $blockDiff = $Last_Seg_Block - $First_Seg_Block;
  if ( ($blockDiff == -0.1) && ($direction eq 'Outbound') )  { # this diffreence is normal for outbound
    $blockDiff = 0;
  }

  # calculate the change from last time
 
  if ( defined($lastTime{"$Info_Q_identifier|$dr"}) ) { # there is data to work with 
    my $td = timeDiff($lastTime{"$Info_Q_identifier|$dr"}, $TS, 'S') ; # get time difference in seconds
    if ( ($td >= 30) || ($rateaverage) ) { # only produce rate information for wait periods > 30 secs or when averaging from the first run
      $sizeChange = $qs - $lastSize{"$Info_Q_identifier|$dr"};
      $changePerSec = '';
      if ( $sizeChange != 0 ) { # calculate how big a change per second it is if the value changed
        $changePerSec = ( $sizeChange * 1024.0 ) / $td; 
        if ( $sizeChange < 0 ) { # queues are reducing in size
          # current time + ((total to process)/((change per second))*60) minutes = estimated time to completion
          $minstogo = int($qs / ($sizeChange / $td * -60)); # note that the -60 is just make the resut a positive number of minutes
          $predict = "Complete at " . timeAdj($TS,$minstogo) . "(" . displayMinutes($minstogo) . ")"
        }
        else {
          $predict = "No completion prediction as queue size is increasing";
        }
      }
    }
  }

  my $highlight = '*****';
  if ( $displayPrediction ) { # add the completion predicton information to the output
    $highlight .= " $predict";
  }

  if ( ($qs > 1) && ($threshold > 1) && ($qs > $threshold) ) { # threshold set and qs exceeds it

    $exitCode = 8; # set the alarm
    $currentAlerts{"$Info_Q_identifier|$dr"} = 1; # flag the fact that something wrong with this one

    printQueueData($TS, $Info_Q_identifier, $disState, $direction, $First_Seg_Block, $Last_Seg_Block , $qs, $Last_Seg_Block - $First_Seg_Block, $sizeChange, $changePerSec, $highlight);
  }
  elsif ( ($blockDiff  > 0) && ( $threshold == 1) ) { # there are blocks on the q

    if ( $exitCode == 0 ) { $exitCode = 2; }

    printQueueData($TS, $Info_Q_identifier, $disState, $direction, $First_Seg_Block, $Last_Seg_Block , $qs, $Last_Seg_Block - $First_Seg_Block, $sizeChange, $changePerSec, $highlight);
  }
  elsif ( $allDevices ) { # display the stable device even if it is empty

      printQueueData($TS, $Info_Q_identifier, $disState, $direction, $First_Seg_Block, $Last_Seg_Block  , $qs, $Last_Seg_Block - $First_Seg_Block, $sizeChange, $changePerSec, '');
  }

  if ( ! $rateaverage ) { # if averaging between snapshots then reset the beginning values 
    $lastTime{"$Info_Q_identifier|$dr"} = $TS;
    $lastSize{"$Info_Q_identifier|$dr"} = $qs;
  }
  else { # only set the beginning value on the first time through
    if ( ! defined($lastTime{"$Info_Q_identifier|$dr"}) ) {
      $lastTime{"$Info_Q_identifier|$dr"} = $TS;
      $lastSize{"$Info_Q_identifier|$dr"} = $qs;
    }
  }

}

sub printQueueData {

  my $TS = shift;
  my $Q = shift;
  my $state = shift;
  my $dir = shift;
  my $F_SB = shift;
  my $L_SB = shift;
  my $qs = shift;
  my $bdiff = shift;
  my $sc = shift;
  my $cps = shift;
  my $lit = shift;

  $displayDiskSize = 1;

  if ( ! defined($lit) ) { $lit = ''; }

  checkQueueHeader();   # check if a header needs printing

  # format the numeric fields ......
  my $t_bdiff = '';
  if ( $bdiff != 0 ) { $t_bdiff = sprintf("%+.3f",$bdiff); }
  my $t_cps = '';
  if ( $cps != 0 ) { $t_cps = sprintf("%+.3f",$cps); }

  printf TMPREP "%-19s %-30s %-20s %10s %11s %11s %11s %10s %6s %10s %5s\n", $TS, $Q, $state, $dir, $F_SB, $L_SB , $qs, $t_bdiff, $sc, $t_cps, $lit;
  printf "%-19s %-30s %-20s %10s %11s %11s %11s %10s %6s %10s %5s\n", $TS, $Q, $state, $dir, $F_SB, $L_SB , $qs, $t_bdiff, $sc, $t_cps, $lit;
  $warningsPrinted = 1;
  $outputLineCount++;
}

sub getQueueSize {

  my $server = shift;
  my $database = shift;

  if ( $debugLevel > 0 ) {print "CMD: ${scriptDir}runSQL.pl -sp ##DATABASE##=$database -f ${scriptDir}checkSQ_2.sql | isql -w 3000 -S $server -U sybmaint -P $tmpPWD -o $tmpOut2"; }
  my $tmp = `${scriptDir}runSQL.pl -sp "##DATABASE##=$database" -f ${scriptDir}checkSQ_2.sql | isql -w 3000 -S $server -U sybmaint -P $tmpPWD -o $tmpOut2`;

  if ( ! open (QSIZE, "<$tmpOut2") ) { die "Unable to open $tmpOut2\n$! \n"; }

  while ( <QSIZE> ) {
    if ( $_ =~ /Logical Connection/ ) { next; } 
    if ( $_ =~ /------------------/ ) { next; } 

    my @piece = split " ";

    if ( substr($piece[1],0,1) eq "I" ) { $queueSize{"$piece[0]|I"} = $piece[2]; }
    else { $queueSize{"$piece[0]|O"} = $piece[2]; }

    # In testing this will allow generation of threshold breaches
    # No effect in Prod as this queue will never be found
    if ( "$piece[0]|O" eq "XXX.iMedCORE|O" ) { $queueSize{"$piece[0]|I"} = 130; }  # In testing this will allow generation of threshold breaches - no effect in prod

    if ( $debugLevel > 0 ) { print "Stored $piece[2] under key $piece[0]|" . substr($piece[1],0,1) . "\n"; }
    
  }

  close QSIZE;

  if ( $debugLevel > 0 ) {
    print ">>>>> PWDLoc: $PWDLoc\n";
    print ">>>>> tmpOut: $tmpOut2\n";
    print ">>>>> tmp: $tmp\n";
  }


}

sub checkDiskHeaderRep {

  if ( $diskHeaderRep ) {
    printf TMPREP "\n%-19s %-10s %-6s %10s %10s %15s %-60s\n", 'Time', 'Queue', 'PartID', 'Total Segs', 'Used Segs', 'State', 'File';
    printf TMPREP "%-19s %-10s %-6s %10s %10s %15s %-60s\n", '-------------------','----------', '------', '----------', '----------', '---------------', '------------------------------------------------------------';

    $diskHeaderRep = 0;
  
  }

}

sub checkDiskHeader {

  if ( $diskHeader ) {
    printf "\n%-19s %-10s %-6s %10s %10s %15s %-60s\n", 'Time', 'Queue', 'PartID', 'Total Segs', 'Used Segs', 'State', 'File';
    printf "%-19s %-10s %-6s %10s %10s %15s %-60s\n", '-------------------','----------', '------', '----------', '----------', '---------------', '------------------------------------------------------------';
  
    $diskHeader = 0;

    $outputLineCount+=1;
    $queueHeader = 1;
    $diskHeaderRep = 1;
    $downHeader = 1;
    $healthHeader = 1;
  }

}

sub checkDownHeader {

  if ( $downHeader ) {
    printf TMPREP "\n%-19s %4s %-10s %-20s %-40s %-20s\n", 'Time', 'SPID', 'Name', 'State', 'Logical Connection', 'Info';
    printf TMPREP "%-19s %4s %-10s %-20s %-40s %-20s\n", '-------------------', '----', '----------', '--------------------', '------------------------------------------', '--------------------';
    printf "\n%-19s %4s %-10s %-20s %-40s %-20s\n", 'Time', 'SPID', 'Name', 'State', 'Logical Connection', 'Info';
    printf "%-19s %4s %-10s %-20s %-40s %-20s\n", '-------------------', '----', '----------', '--------------------', '----------------------------------------', '--------------------';

    $outputLineCount+=3;
    $downHeader = 0;

    $queueHeader = 1;
    $diskHeaderRep = 1;
    $diskHeader = 1;
    $healthHeader = 1;
  }

}

sub checkQueueHeader {

  if ( $queueHeader || ($outputLineCount > $pageSize) ) {
    printf TMPREP "\n%-19s %-30s %-20s %10s %11s %11s %11s %10s %6s %10s \n", 'Time', 'Logical Connection', '', 'Direction', 'First Block', 'Last Block', 'Size (Mb)', 'Block Diff', 'Change', 'Kb/Sec';
    printf "\n%-19s %-30s %-20s %10s %11s %11s %11s %10s %6s %10s \n", 'Time', 'Logical Connection', '', 'Direction', 'First Block', 'Last Block', 'Size (Mb)', 'Block Diff', 'Change', 'Kb/Sec';
    printf TMPREP "%-19s %-30s %-20s %10s %11s %11s %11s %10s %6s %10s \n", '-------------------', '------------------------------', '--------------------', '----------', '-----------', '-----------', '-----------', '----------', '------', '----------';
    printf "%-19s %-30s %-20s %10s %11s %11s %11s %10s %6s %10s \n", '-------------------', '------------------------------', '--------------------', '----------', '-----------', '-----------', '-----------', '----------', '------', '----------';

    if ($Info_Q_identifier =~ /RSSD/ ) { # check if this is the database connection
        print TMPREP "                    Related RSSD Server: $RSSD_server, RSSD database: $RSSD_database\n";
        print "                    Related RSSD Server: $RSSD_server, RSSD database: $RSSD_database\n";
    }

    $outputLineCount=3;
    $queueHeader = 0;

    $diskHeaderRep = 1;
    $diskHeader = 1;
    $downHeader = 1;
    $healthHeader = 1;
  }

}

sub getTimestamp {

  my ($second, $minute, $hour, $dayOfMonth, $month, $yearOffset, $dayOfWeek, $dayOfYear, $daylightSavings) = localtime();
  my $year = 1900 + $yearOffset;
  $month = $month + 1;
  $hour = substr("0" . $hour, length($hour)-1,2);
  $minute = substr("0" . $minute, length($minute)-1,2);
  $second = substr("0" . $second, length($second)-1,2);
  $month = substr("0" . $month, length($month)-1,2);
  my $day = substr("0" . $dayOfMonth, length($dayOfMonth)-1,2);
  my $NowTS = "$year.$month.$day $hour:$minute:$second";

  return $NowTS;

}

exit $exitCode;

