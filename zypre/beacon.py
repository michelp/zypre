from gevent import monkey
monkey.patch_all()

import logging
import socket
import struct
import errno
import uuid
import time
from collections import namedtuple

import gevent
from zmq import green as zmq

beaconv1 = struct.Struct('3sB16sH')
beaconv2 = struct.Struct('3sB16sHBB4s')

Peer = namedtuple('Peer', ['socket', 'addr', 'time'])

log = logging.getLogger(__name__)

T_TO_I = {'tcp': 1,
          'pgm': 2,
          }

I_TO_T = {v: k for k, v in T_TO_I.items()}

NULL_IP = '\x00' * 4


class Beaconer(object):
    """ZRE beacon emmiter.  http://rfc.zeromq.org/spec:20

    This implements only the base UDP beaconing 0mq socket
    interconnection layer, and disconnected peer detection.
    """
    service_port = None

    def __init__(self, callback,
                 broadcast_addr='',
                 broadcast_port=35713,
                 service_addr='*',
                 service_transport='tcp',
                 service_socket_type=zmq.ROUTER,
                 beacon_interval=1,
                 dead_interval=30):
        self.callback = callback
        self.broadcast_addr = broadcast_addr
        self.broadcast_port = broadcast_port
        self.service_addr = service_addr
        self.service_transport = service_transport
        self.service_socket_type = service_socket_type
        self.beacon_interval = beacon_interval
        self.dead_interval = dead_interval

        self.peers = {}
        if service_addr != '*':
            self.service_addr_bytes = socket.inet_aton(
                socket.gethostbyname(service_addr))
        else:
            self.service_addr_bytes = NULL_IP
        self.me = uuid.uuid4().bytes

    def start(self):
        """Greenlet to start the beaconer.  This sets up zmq context,
        sockets, and spawns worker greenlets.
        """
        self.context = zmq.Context()
        self.router = self.context.socket(self.service_socket_type)
        endpoint = '%s://%s' % (self.service_transport, self.service_addr)
        self.service_port = self.router.bind_to_random_port(endpoint)

        self.broadcaster = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM,
            socket.IPPROTO_UDP)

        self.broadcaster.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_BROADCAST,
            2)

        self.broadcaster.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1)

        self.broadcaster.bind((self.broadcast_addr, self.broadcast_port))

        # start all worker greenlets
        gevent.joinall(
            [gevent.spawn(self._send_beacon),
             gevent.spawn(self._recv_beacon),
             gevent.spawn(self._recv_msg)])

    def _recv_beacon(self):
        """Greenlet that receives udp beacons.
        """
        while True:
            try:
                data, (peer_addr, port) = self.broadcaster.recvfrom(beaconv2.size)
            except socket.error:
                log.exception('Error recving beacon:')
                gevent.sleep(self.beacon_interval) # don't busy error loop
                continue
            if len(data) == beaconv1.size:
                continue
            try:
                greet, ver, peer_id, peer_port, peer_transport, \
                    peer_socket_type, peer_socket_address = beaconv2.unpack(data)
            except Exception:
                continue
            if greet != 'ZRE':
                continue
            if peer_id == self.me:
                continue
            if peer_socket_address != NULL_IP:
                peer_addr = socket.inet_ntoa(peer_socket_address)
            peer_transport = I_TO_T[peer_transport]
            self.handle_beacon(peer_id, peer_transport, peer_addr,
                               peer_port, peer_socket_type)

    def _send_beacon(self):
        """Greenlet that sends udp beacons at intervals.
        """
        while True:
            try:
                beacon = beaconv2.pack(
                    'ZRE', 1, self.me,
                    self.service_port,
                    T_TO_I[self.service_transport],
                    self.service_socket_type,
                    self.service_addr_bytes)

                self.broadcaster.sendto(
                    beacon,
                    ('<broadcast>', self.broadcast_port))
            except socket.error:
                log.exception('Error sending beacon:')
            gevent.sleep(self.beacon_interval)
            # check for deadbeats
            now = time.time()
            for peer_id in self.peers.keys():
                peer = self.peers[peer_id]
                if now - peer.time > self.dead_interval:
                    log.debug('Deadbeat: %s' % uuid.UUID(bytes=peer_id))
                    peer.socket.close()
                    del self.peers[peer_id]

    def _recv_msg(self):
        """Greenlet that receives messages from the local ROUTER
        socket.
        """
        while True:
            self.handle_msg(*self.router.recv_multipart())

    def handle_beacon(self, peer_id, transport, addr, port, socket_type):
        """ Handle a beacon.

        Overide this method to handle new peers.  By default, connects
        a DEALER socket to the new peers broadcast endpoint and
        registers it.
        """
        peer_addr = '%s://%s:%s' % (transport, addr, port)
        peer = self.peers.get(peer_id)
        if peer and peer.addr == peer_addr:
            self.peers[peer_id] = peer._replace(time=time.time())
            return
        elif peer:
            # we have the peer, but it's addr changed,
            # close it, we'll reconnect
            self.peers[peer_id].socket.close()

        # connect DEALER to peer_addr address from beacon
        peer = self.context.socket(zmq.DEALER)
        peer.setsockopt(zmq.IDENTITY, self.me)

        uid = uuid.UUID(bytes=peer_id)
        log.info('conecting to: %s at %s' % (uid, peer_addr))
        peer.connect(peer_addr)
        self.peers[peer_id] = Peer(peer, peer_addr, time.time())

    def handle_msg(self, peer_id, msg):
        """Override this method to customize message handling.

        Defaults to calling the callback.
        """
        peer = self.peers.get(peer_id)
        if peer:
            self.peers[peer_id] = peer = peer._replace(time=time.time())
            gevent.spawn(self.callback, self, peer, msg)


if __name__ == '__main__':
    import sys
    logging.basicConfig(level=logging.DEBUG)

    def my_callback(pyre, peer, msg):
        time.sleep(1)
        print msg

    p = Beaconer(my_callback)
    g = gevent.spawn(p.start)
    s = gevent.sleep
    if len(sys.argv) > 1:
        g.join()
