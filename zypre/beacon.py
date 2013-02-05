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

beacon = struct.Struct('3sB16sH')
Peer = namedtuple('Peer', ['socket', 'addr', 'time'])

log = logging.getLogger(__name__)


class Beaconer(object):
    """ZRE beacon emmiter.  http://rfc.zeromq.org/spec:20

    This implements only the base UDP beaconing 0mq socket
    interconnection layer, and disconnected peer detection.
    """

    def __init__(self, callback, address='*', broadcast_port=35713,
                 beacon_interval=1, dead_interval=60):
        self.callback = callback
        self.endpoint = 'tcp://%s' % address
        self.broadcast_port = broadcast_port
        self.beacon_interval = beacon_interval
        self.dead_interval = dead_interval
        self.peers = {}

    def start(self):
        """Greenlet to start the beaconer.  This sets up zmq context,
        sockets, and spawns worker greenlets.
        """
        self.context = zmq.Context()
        self.router = self.context.socket(zmq.ROUTER)
        self.port = self.router.bind_to_random_port(self.endpoint)
        self.me = uuid.uuid4().bytes

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

        self.broadcaster.bind(('', self.broadcast_port))

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
                data, addr = self.broadcaster.recvfrom(beacon.size)
            except socket.error:
                log.exception('Error recving beacon:')
                gevent.sleep(self.beacon_interval) # don't busy error loop
                continue
            try:
                greet, ver, peer_id, peer_port = beacon.unpack(data)
            except Exception:
                continue
            if greet != 'ZRE':
                continue
            if peer_id == self.me:
                continue
            self.handle_beacon(peer_id, addr[0], peer_port)

    def _send_beacon(self):
        """Greenlet that sends udp beacons at intervals.
        """
        while True:
            try:
                self.broadcaster.sendto(
                    beacon.pack('ZRE', 1, self.me, self.port),
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
            self.handle_msg(self.router.recv_multipart())

    def handle_beacon(self, peer_id, addr, port):
        """ Handle a beacon.

        Overide this method to handle new peers.  By default, connects
        a DEALER socket to the new peers broadcast endpoint and
        registers it.
        """
        peer_addr = 'tcp://%s:%s' % (addr, port)
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
