# asterisk_oracle_realtime_engine

Realtime oracle engine for both static configuration and dyanamic configuration
Use ocilib to compile this module

I have developed this realtime oracle engine for the asterisk 1.4 version (1.4 to 1.4.*) but not tested with other version.

This is a new feature of realtime engine. it support oracle database connectivity to load sip,voicemail,queue etc from db table.

you have to put this file directly in res folder of asterisk.

change Makefile and append following line to SOLINk -L$ORACLE_HOME/lib/ -lclntsh -L/usr/local/lib -locilib -locci

then again compile asterisk.
if you use client then set ORACLE_HOME path in environment var.

here ,my oracle lib path is -L$ORACLE_HOME/lib/ -lclntsh
