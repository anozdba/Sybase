#!/bin/bash
#-----------------------------------------------------------------------
# checkLatency.ksh - check the latency of the replicated connections
#
# $Id: checkLatency.ksh,v 1.6 2017/11/06 00:54:49 db2admin Exp db2admin $
#
#
# Description:
#
# Uses checkLatency.pl to monitor the latency of the connections
#
# $Name:  $
#
# Change Log:
# $Log: checkLatency.ksh,v $
# Revision 1.6  2017/11/06 00:54:49  db2admin
# add in parameters d and T so that they can be passed to the perl script
#
# Revision 1.5  2017/06/27 22:04:19  db2admin
# add in group option
#
# Revision 1.4  2017/06/19 23:27:01  db2admin
# correct selection of command line option -i
#
# Revision 1.3  2017/06/07 23:13:22  db2admin
# parameterise all of the parameters to checkLatency.pl
#
# Revision 1.2  2017/06/07 05:33:04  db2admin
# change name to standard name
#
#-----------------------------------------------------------------------

# Load Sybase Environment settings
. $HOME/syb_env

# Usage command
usage () {

     rc=0

#   If a parameter has been passed then echo it
     [[ $# -gt 0 ]] && { echo "${0##*/}: $*" 1>&2; rc=1; }

     cat <<-EOF 1>&2
   Usage: checkLatency.ksh {-h] -S <server name> [-t <threshold>] [-T <delay threshold>] [-d <alert delay>] [-i <iterations>] [-w <wait in seconds>] [-e <email to alert to>] [-g <group>]

      -h      : this message
      -s      : server name to be used
      -t      : threshold to alert on in seconds (default 60)
      -g      : database group [no default]
      -T      : threshold to alert on if a heartbeat has been delayed by this amount in seconds (default 600)
      -d      : delay in minutes to be enforced for identical alerts (defaults to 240)
      -i      : number of iterations (default 5000)
      -w      : wait time between tests (default 300)
      -e      : email to alert to (default: none will be used so will default to default in checkLatency.pl)

   Script to check the latency on the replicated streams on the specified server

EOF

     exit $rc

}

#-----------------------------------------------------------------------
# Set defaults and parse command line

# Default settings
server=""
wait=300
threshold=60
delayThreshold=600
email=""
group=""
iterations=5000
alertDelay=240

# Check command line options
while getopts ":hS:g:w:t:e:i:T:d:" opt; do
     case $opt in

         #  What server to use
         S) server="$OPTARG" 
            serverName="$OPTARG"
            ;;

         #  Threshold to alert onlert Delay
         d) alertDelay="$OPTARG" 
            ;;

         #  Threshold to alert on
         t) threshold="$OPTARG" 
            ;;

         #  Delay Threshold to alert on
         T) delayThreshold="$OPTARG" 
            ;;

         #  Group
         g) group="-g $OPTARG" 
            ;;

         #  Wait time in seconds
         w) wait="$OPTARG" 
            ;;

         #  Number of iterations
         i) iterations="$OPTARG" 
            ;;

         #  Email address to use
         e) email="-e $OPTARG" 
            ;;

         # Print out the usage information
         h)  usage ''
             return 1 ;;

         *)  usage 'invalid option(s)'
             return 1 ;;
     esac
done
shift $(($OPTIND - 1))      # get rid of any parameters processed by getopts

# assign parameters if not explicitly assigned

for i in "$@"
do
   server="$i"
   shift
done

# end of parameter section
#-----------------------------------------------------------------------

# Load functions
if [[ -s $DBA_SCRIPTS_DIR/dba_funcs.sh ]]; then
    . $DBA_SCRIPTS_DIR/dba_funcs.sh
else
    echo "dba_funcs.sh file cannot be found. Fatal Error script exiting!"
    exit 100
fi

if [[ -z $server ]]; then
    usage 'server name required'
else
    # Set some things up
    setSybaseEnvironment
fi

exec >$DBA_LOG_DIR/checkLatency.log

#/prj/sybase/app/sybmaint/scripts/checkLatency.pl -S $server -t 60 -n 5000 -w 60

# /prj/sybase/app/sybmaint/scripts/checkLatency.pl -S $server -D 'iMedWORK,iMedETL,iMedCONV,iMedSOURCE,iMedSTAGE,iMedCORE,iMedINTER,iMedQUOTE,master' -t 60 -n 5000 -w 60 -e webmaster@KAGJCM.com.au

/prj/sybase/app/sybmaint/scripts/checkLatency.pl -S $server  -f monitoringList -t $threshold -T $delayThreshold -d $alertDelay -n $iterations -w $wait $group $email

