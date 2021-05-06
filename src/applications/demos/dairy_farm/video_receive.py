import socket
import cv2
import pickle
import struct
import sys
import numpy as np
import cascade_py
import time

capi = cascade_py.ServiceClientAPI()

def receive_frames_from_server(host_ip, port):
    receive_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    receive_socket.bind((host_ip, port))
    print("[SOCKET] bind complete")

    receive_socket.listen(10)
    print("[SOCKET] listening ")
    conn, addr = receive_socket.accept()

    data = b""
    payload_size = struct.calcsize("Q")
    print("payload_size: {}".format(payload_size))
    while True:
        try:
            while len(data) < payload_size:
                # use 4b buffer
                data += conn.recv(4096)

            print("Done Recv: {}".format(len(data)))
            packed_msg_size = data[:payload_size]
            data = data[payload_size:]
            msg_size = struct.unpack("Q", packed_msg_size)[0]
            while len(data) < msg_size:
                data += conn.recv(4096)
            frame_data = data[:msg_size]
            data = data[msg_size:]
            frame_packet = pickle.loads(frame_data)
            ts,idx = frame_packet['header']
            image_frame = frame_packet['frame']

            img = cv2.imdecode(image_frame, cv2.IMREAD_COLOR)
            img = cv2.resize(img, (352,240))
            img = img / 255
            img = img.astype(np.single)
            cascade_frame = img.tobytes()

            extract_ts = "{:.0f}".format(ts*(10**9))
            frame_id = '/dairy_farm/compute/'+str(idx)+'_'+extract_ts
            print(frame_id)
            ret = capi.put('VCSS', frame_id, cascade_frame, 0, 0)
            print(ret.get_result())
        except Exception as e:
            print('exception occured: {}'.format(e))
    receive_socket.close()


if __name__ == '__main__':
    host_ip = ''
    port = 8081
    receive_frames_from_server(host_ip, port)