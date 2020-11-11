#!/usr/bin/python3

import sys
import telnetlib

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print ("Usage: %s <host> <port>" % sys.argv[0])
        quit()
    tn = telnetlib.Telnet(sys.argv[1],int(sys.argv[2]))
    tn.write(b"shutdown")
    tn.read_until(b"shutdown")
    tn.close()
    print("Shutdown acknowledged by %s:%s" % (sys.argv[1],sys.argv[2]))

