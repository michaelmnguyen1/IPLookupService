#!/bin/sh
#
# myservice     This shell script takes care of starting and stopping
#               the /home/webreports/report-listen
#

# Source function library
. /etc/rc.d/init.d/functions


# Do preliminary checks here, if any
#### START of preliminary checks #########


##### END of preliminary checks #######


# Handle manual control parameters like start, stop, status, restart, etc.

case "$1" in
  start)
    # Start daemons.

    echo -n $"Starting Spark JDBC service: "
    echo
    # daemon /home/webreports/report-listen
    echo
    ;;

  stop)
    # Stop daemons.
    echo -n $"Shutting down Cake Spark JDBC service: "
    # killproc /home/webreports/report-listen
    echo

    # Do clean-up works here like removing pid files from /var/run, etc.
    ;;
  status)
    echo -n $"Status Cake Spark JDBC service: "
    status /home/webreports/report-listen

    ;;
  restart)
    $0 stop
    $0 start
    ;;

  *)
    echo $"Usage: $0 {start|stop|status|restart}"
    exit 1
esac

exit 0