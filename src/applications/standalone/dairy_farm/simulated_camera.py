#!/usr/bin/env python3
import sys
import cv2
import argparse
import time
import signal
import logging
import os
import numpy as np
try:
    import cascade_py
except ModuleNotFoundError:
    print ("Failed to load a cascade_py.*.so implementation. Please either install cascade python support, or set $PYTHONPATH to the folder containing the built cascade_py.*.so")
    sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument('-s', '--source',
                    metavar="<video source>",
                    required=True,
                    help='Specify the video source, it can be a name of a video file, an image sequence, or URL of a video stream. Please refer to cv2::VideoCapture() API for more information.')
parser.add_argument('-r', '--rate',
                    metavar="<rate in fps>",
                    required=False,
                    default=1.0,
                    help='frame sampling rate in fps.')
parser.add_argument('-p', '--path',
                    metavar="<object path in cascade filesystem>",
                    required=True,
                    help='Specify the path in the Cascade filesystem where the photos will be send to.')
parser.add_argument('-c', '--camera',
                    metavar="<name of the camera>",
                    required=False,
                    default='default_camera',
                    help='Specify the name of the camera. The frames will be send to \'<path>/<camera>\' folder by trigger put')
parser.add_argument('-l', '--logging_level',
                    metavar="<logging level>",
                    required=False,
                    default='INFO',
                    help='Specify the loglevel. choices: DEBUG|INFO|WARNING|ERROR|CRITICAL')
parser.add_argument('-f', '--logfile',
                    metavar="<logging file>",
                    required=False,
                    default=None,
                    help='Specify the log file. By default, log is put to console.')
def create_payload(frame,photo_id=0):
    frame = cv2.resize(frame, (352,240))
    frame = frame/255.0
    frame = frame.astype(np.float32)
    header = np.array([photo_id,int(time.time_ns()/1000)],dtype=np.uint64)
    return header.tobytes() + frame.tobytes()

if __name__ == '__main__':
    args = parser.parse_args()
    # logging
    if args.logfile is not None:
        logging.basicConfig(level=getattr(logging,args.logging_level),filename=args.logfile)
    else:
        logging.basicConfig(level=getattr(logging,args.logging_level))
    # capture video
    cap = cv2.VideoCapture(args.source)
    fps = cap.get(cv2.CAP_PROP_FPS);
    sampling_rate = min(float(args.rate), fps)
    logging.info("FPS %f, Sampling rate=%f" % (fps,sampling_rate))
    interval_sec = 1.0 / fps
    sampling_interval_sec = 1.0 / sampling_rate
    i = 0
    # frame path
    key = args.path
    if key.endswith(os.path.sep):
        key = key + args.camera
    else:
        key = key + os.path.sep + args.camera
    # register ctrl-c signal
    def ctrl_c_handler(sig, frame):
        print ("Signal Received: sig=%d, Clean up." % sig)
        if (cap.isOpened):
            cap.release()
            cv2.destroyAllWindows()
        sys.exit(0)
    signal.signal(signal.SIGINT,ctrl_c_handler)
    # prepare external client
    logging.info("Connecting to Cascade Service...")
    capi = cascade_py.ServiceClientAPI()
    logging.info("Connected to Cascade Service")
    nxt = time.time()
    nxt_sample = time.time()
    while (cap.isOpened):
        cur = time.time()
        if cur < nxt:
            time.sleep(nxt-cur)
        nxt = time.time() + interval_sec
        ret, frame = cap.read()
        i = i+1
        if ret == False:
            break
        cur = time.time()
        if cur + interval_sec < nxt_sample:
            continue;
        nxt_sample = cur + sampling_interval_sec
        logging.info("Write frame %d to %s..." % (i,key))
        capi.trigger_put('TriggerCascadeNoStoreWithStringKey',key,create_payload(frame,i))
        logging.info("frame %d written." % i)
    cap.release()
    cv2.destroyAllWindows()
