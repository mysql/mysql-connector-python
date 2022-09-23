# Copyright (c) 2012, 2022, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0, as
# published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an
# additional permission to link the program and your derivative works
# with the separately licensed software that they have included with
# MySQL.
#
# Without limiting anything contained in the foregoing, this file,
# which is part of MySQL Connector/Python, is also subject to the
# Universal FOSS Exception, version 1.0, a copy of which can be found at
# http://oss.oracle.com/licenses/universal-foss-exception.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

# mypy: disable-error-code="attr-defined"

"""Module implementing low-level socket communication with MySQL servers.
"""

import os
import socket
import struct
import warnings
import zlib

from collections import deque

try:
    import ssl

    TLS_VERSIONS = {
        "TLSv1": ssl.PROTOCOL_TLSv1,
        "TLSv1.1": ssl.PROTOCOL_TLSv1_1,
        "TLSv1.2": ssl.PROTOCOL_TLSv1_2,
    }
    # TLSv1.3 included in PROTOCOL_TLS, but PROTOCOL_TLS is not included on 3.4
    TLS_VERSIONS["TLSv1.3"] = (
        ssl.PROTOCOL_TLS
        if hasattr(ssl, "PROTOCOL_TLS")
        else ssl.PROTOCOL_SSLv23  # Alias of PROTOCOL_TLS
    )
    TLS_V1_3_SUPPORTED = hasattr(ssl, "HAS_TLSv1_3") and ssl.HAS_TLSv1_3
except ImportError:
    # If import fails, we don't have SSL support.
    TLS_V1_3_SUPPORTED = False

from typing import Any, Deque, List, Optional, Tuple, Union

from .constants import MAX_PACKET_LENGTH
from .errors import InterfaceError, NotSupportedError, OperationalError
from .types import StrOrBytesPath
from .utils import init_bytearray


def _strioerror(err: IOError) -> str:
    """Reformat the IOError error message

    This function reformats the IOError error message.
    """
    if not err.errno:
        return str(err)
    return f"{err.errno} {err.strerror}"


def _prepare_packets(buf: bytes, pktnr: int) -> List[bytes]:
    """Prepare a packet for sending to the MySQL server"""
    pkts = []
    pllen = len(buf)
    maxpktlen = MAX_PACKET_LENGTH
    while pllen > maxpktlen:
        pkts.append(b"\xff\xff\xff" + struct.pack("<B", pktnr) + buf[:maxpktlen])
        buf = buf[maxpktlen:]
        pllen = len(buf)
        pktnr = pktnr + 1
    pkts.append(struct.pack("<I", pllen)[0:3] + struct.pack("<B", pktnr) + buf)
    return pkts


class BaseMySQLSocket:
    """Base class for MySQL socket communication

    This class should not be used directly but overloaded, changing the
    at least the open_connection()-method. Examples of subclasses are
      mysql.connector.network.MySQLTCPSocket
      mysql.connector.network.MySQLUnixSocket
    """

    def __init__(self) -> None:
        # holds the socket connection
        self.sock: Optional[socket.socket] = None
        self._connection_timeout: Optional[int] = None
        self._packet_number: int = -1
        self._compressed_packet_number: int = -1
        self._packet_queue: Deque[bytearray] = deque()
        self.server_host: Optional[str] = None
        self.recvsize: int = 8192

    def next_packet_number(self) -> int:
        """Increments the packet number"""
        self._packet_number = self._packet_number + 1
        if self._packet_number > 255:
            self._packet_number = 0
        return self._packet_number

    def next_compressed_packet_number(self) -> int:
        """Increments the compressed packet number"""
        self._compressed_packet_number = self._compressed_packet_number + 1
        if self._compressed_packet_number > 255:
            self._compressed_packet_number = 0
        return self._compressed_packet_number

    def open_connection(self) -> Any:
        """Open the socket"""
        raise NotImplementedError

    def get_address(self) -> Any:
        """Get the location of the socket"""
        raise NotImplementedError

    def shutdown(self) -> None:
        """Shut down the socket before closing it"""
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
            del self._packet_queue
        except (AttributeError, OSError):
            pass

    def close_connection(self) -> None:
        """Close the socket"""
        try:
            self.sock.close()
            del self._packet_queue
        except (AttributeError, OSError):
            pass

    def __del__(self) -> None:
        self.shutdown()

    def send_plain(
        self,
        buf: bytes,
        packet_number: Optional[int] = None,
        compressed_packet_number: Optional[int] = None,
    ) -> None:
        """Send packets to the MySQL server"""
        # Keep 'compressed_packet_number' for API backward compatibility
        _ = compressed_packet_number
        if packet_number is None:
            self.next_packet_number()
        else:
            self._packet_number = packet_number
        packets = _prepare_packets(buf, self._packet_number)
        for packet in packets:
            try:
                self.sock.sendall(packet)
            except IOError as err:
                raise OperationalError(
                    errno=2055, values=(self.get_address(), _strioerror(err))
                ) from err
            except AttributeError as err:
                raise OperationalError(errno=2006) from err

    send = send_plain

    def send_compressed(
        self,
        buf: bytes,
        packet_number: Optional[int] = None,
        compressed_packet_number: Optional[int] = None,
    ) -> None:
        """Send compressed packets to the MySQL server"""
        if packet_number is None:
            self.next_packet_number()
        else:
            self._packet_number = packet_number
        if compressed_packet_number is None:
            self.next_compressed_packet_number()
        else:
            self._compressed_packet_number = compressed_packet_number

        pktnr = self._packet_number
        pllen = len(buf)
        zpkts = []
        maxpktlen = MAX_PACKET_LENGTH
        if pllen > maxpktlen:
            pkts = _prepare_packets(buf, pktnr)
            tmpbuf = b"".join(pkts)
            del pkts
            zbuf = zlib.compress(tmpbuf[:16384])
            header = (
                struct.pack("<I", len(zbuf))[0:3]
                + struct.pack("<B", self._compressed_packet_number)
                + b"\x00\x40\x00"
            )
            zpkts.append(header + zbuf)
            tmpbuf = tmpbuf[16384:]
            pllen = len(tmpbuf)
            self.next_compressed_packet_number()
            while pllen > maxpktlen:
                zbuf = zlib.compress(tmpbuf[:maxpktlen])
                header = (
                    struct.pack("<I", len(zbuf))[0:3]
                    + struct.pack("<B", self._compressed_packet_number)
                    + b"\xff\xff\xff"
                )
                zpkts.append(header + zbuf)
                tmpbuf = tmpbuf[maxpktlen:]
                pllen = len(tmpbuf)
                self.next_compressed_packet_number()
            if tmpbuf:
                zbuf = zlib.compress(tmpbuf)
                header = (
                    struct.pack("<I", len(zbuf))[0:3]
                    + struct.pack("<B", self._compressed_packet_number)
                    + struct.pack("<I", pllen)[0:3]
                )
                zpkts.append(header + zbuf)
            del tmpbuf
        else:
            pkt = struct.pack("<I", pllen)[0:3] + struct.pack("<B", pktnr) + buf
            pllen = len(pkt)
            if pllen > 50:
                zbuf = zlib.compress(pkt)
                zpkts.append(
                    struct.pack("<I", len(zbuf))[0:3]
                    + struct.pack("<B", self._compressed_packet_number)
                    + struct.pack("<I", pllen)[0:3]
                    + zbuf
                )
            else:
                header = (
                    struct.pack("<I", pllen)[0:3]
                    + struct.pack("<B", self._compressed_packet_number)
                    + struct.pack("<I", 0)[0:3]
                )
                zpkts.append(header + pkt)

        for zip_packet in zpkts:
            try:
                self.sock.sendall(zip_packet)
            except IOError as err:
                raise OperationalError(
                    errno=2055, values=(self.get_address(), _strioerror(err))
                ) from err
            except AttributeError as err:
                raise OperationalError(errno=2006) from err

    def recv_plain(self) -> bytearray:
        """Receive packets from the MySQL server"""
        try:
            # Read the header of the MySQL packet, 4 bytes
            packet = bytearray(b"")
            packet_len = 0
            while packet_len < 4:
                chunk = self.sock.recv(4 - packet_len)
                if not chunk:
                    raise InterfaceError(errno=2013)
                packet += chunk
                packet_len = len(packet)

            # Save the packet number and payload length
            self._packet_number = packet[3]
            payload_len = struct.unpack("<I", packet[0:3] + b"\x00")[0]

            # Read the payload
            rest = payload_len
            packet.extend(bytearray(payload_len))
            packet_view = memoryview(packet)
            packet_view = packet_view[4:]
            while rest:
                read = self.sock.recv_into(packet_view, rest)
                if read == 0 and rest > 0:
                    raise InterfaceError(errno=2013)
                packet_view = packet_view[read:]
                rest -= read
            return packet
        except IOError as err:
            raise OperationalError(
                errno=2055, values=(self.get_address(), _strioerror(err))
            ) from err

    recv = recv_plain

    def _split_zipped_payload(self, packet_bunch: bytearray) -> None:
        """Split compressed payload"""
        while packet_bunch:
            payload_length = struct.unpack("<I", packet_bunch[0:3] + b"\x00")[0]
            self._packet_queue.append(packet_bunch[0 : payload_length + 4])
            packet_bunch = packet_bunch[payload_length + 4 :]

    def recv_compressed(self) -> Optional[bytearray]:
        """Receive compressed packets from the MySQL server"""
        try:
            pkt = self._packet_queue.popleft()
            self._packet_number = pkt[3]
            return pkt
        except IndexError:
            pass

        header = bytearray(b"")
        packets = []
        try:
            abyte = self.sock.recv(1)
            while abyte and len(header) < 7:
                header += abyte
                abyte = self.sock.recv(1)
            while header:
                if len(header) < 7:
                    raise InterfaceError(errno=2013)

                # Get length of compressed packet
                zip_payload_length = struct.unpack("<I", header[0:3] + b"\x00")[0]
                self._compressed_packet_number = header[3]

                # Get payload length before compression
                payload_length = struct.unpack("<I", header[4:7] + b"\x00")[0]

                zip_payload = init_bytearray(abyte)
                while len(zip_payload) < zip_payload_length:
                    chunk = self.sock.recv(zip_payload_length - len(zip_payload))
                    if not chunk:
                        raise InterfaceError(errno=2013)
                    zip_payload = zip_payload + chunk

                # Payload was not compressed
                if payload_length == 0:
                    self._split_zipped_payload(zip_payload)
                    pkt = self._packet_queue.popleft()
                    self._packet_number = pkt[3]
                    return pkt

                packets.append((payload_length, zip_payload))

                if zip_payload_length <= 16384:
                    # We received the full compressed packet
                    break

                # Get next compressed packet
                header = init_bytearray(b"")
                abyte = self.sock.recv(1)
                while abyte and len(header) < 7:
                    header += abyte
                    abyte = self.sock.recv(1)

        except IOError as err:
            raise OperationalError(
                errno=2055, values=(self.get_address(), _strioerror(err))
            ) from err

        # Compressed packet can contain more than 1 MySQL packets
        # We decompress and make one so we can split it up
        tmp = init_bytearray(b"")
        for payload_length, payload in packets:
            # payload_length can not be 0; this was previously handled
            tmp += zlib.decompress(payload)
        self._split_zipped_payload(tmp)
        del tmp

        try:
            pkt = self._packet_queue.popleft()
            self._packet_number = pkt[3]
            return pkt
        except IndexError:
            pass
        return None

    def set_connection_timeout(self, timeout: Optional[int]) -> None:
        """Set the connection timeout"""
        self._connection_timeout = timeout
        if self.sock:
            self.sock.settimeout(timeout)

    def switch_to_ssl(
        self,
        ca: StrOrBytesPath,
        cert: StrOrBytesPath,
        key: StrOrBytesPath,
        verify_cert: bool = False,
        verify_identity: bool = False,
        cipher_suites: Optional[str] = None,
        tls_versions: Optional[List[str]] = None,
    ) -> None:
        """Switch the socket to use SSL"""
        if not self.sock:
            raise InterfaceError(errno=2048)

        try:
            if verify_cert:
                cert_reqs = ssl.CERT_REQUIRED
            elif verify_identity:
                cert_reqs = ssl.CERT_OPTIONAL
            else:
                cert_reqs = ssl.CERT_NONE

            if tls_versions is None or not tls_versions:
                context = ssl.create_default_context()
                if not verify_identity:
                    context.check_hostname = False
            else:
                tls_versions.sort(reverse=True)

                tls_version = tls_versions[0]
                if (
                    not TLS_V1_3_SUPPORTED
                    and tls_version == "TLSv1.3"
                    and len(tls_versions) > 1
                ):
                    tls_version = tls_versions[1]
                ssl_protocol = TLS_VERSIONS[tls_version]
                context = ssl.SSLContext(ssl_protocol)

                if tls_version == "TLSv1.3":
                    if "TLSv1.2" not in tls_versions:
                        context.options |= ssl.OP_NO_TLSv1_2
                    if "TLSv1.1" not in tls_versions:
                        context.options |= ssl.OP_NO_TLSv1_1
                    if "TLSv1" not in tls_versions:
                        context.options |= ssl.OP_NO_TLSv1

            context.check_hostname = False
            context.verify_mode = cert_reqs
            context.load_default_certs()

            if ca:
                try:
                    context.load_verify_locations(ca)
                except (IOError, ssl.SSLError) as err:
                    self.sock.close()
                    raise InterfaceError(f"Invalid CA Certificate: {err}") from err
            if cert:
                try:
                    context.load_cert_chain(cert, key)
                except (IOError, ssl.SSLError) as err:
                    self.sock.close()
                    raise InterfaceError(f"Invalid Certificate/Key: {err}") from err
            if cipher_suites:
                context.set_ciphers(cipher_suites)

            if hasattr(self, "server_host"):
                self.sock = context.wrap_socket(
                    self.sock, server_hostname=self.server_host
                )
            else:
                self.sock = context.wrap_socket(self.sock)

            if verify_identity:
                context.check_hostname = True
                hostnames: List[str] = [self.server_host] if self.server_host else []
                if os.name == "nt" and self.server_host == "localhost":
                    hostnames = ["localhost", "127.0.0.1"]
                    aliases = socket.gethostbyaddr(self.server_host)
                    hostnames.extend([aliases[0]] + aliases[1])
                match_found = False
                errs = []
                for hostname in hostnames:
                    try:
                        # Deprecated in Python 3.7 without a replacement and
                        # should be removed in the future, since OpenSSL now
                        # performs hostname matching
                        # pylint: disable=deprecated-method
                        ssl.match_hostname(self.sock.getpeercert(), hostname)
                        # pylint: enable=deprecated-method
                    except ssl.CertificateError as err:
                        errs.append(str(err))
                    else:
                        match_found = True
                        break
                if not match_found:
                    self.sock.close()
                    raise InterfaceError(
                        f"Unable to verify server identity: {', '.join(errs)}"
                    )
        except NameError as err:
            raise NotSupportedError("Python installation has no SSL support") from err
        except (ssl.SSLError, IOError) as err:
            raise InterfaceError(
                errno=2055, values=(self.get_address(), _strioerror(err))
            ) from err
        except ssl.CertificateError as err:
            raise InterfaceError(str(err)) from err
        except NotImplementedError as err:
            raise InterfaceError(str(err)) from err


class MySQLUnixSocket(BaseMySQLSocket):
    """MySQL socket class using UNIX sockets

    Opens a connection through the UNIX socket of the MySQL Server.
    """

    def __init__(self, unix_socket: str = "/tmp/mysql.sock") -> None:
        super().__init__()
        self.unix_socket: str = unix_socket

    def get_address(self) -> str:
        return self.unix_socket

    def open_connection(self) -> None:
        try:
            self.sock = socket.socket(
                socket.AF_UNIX, socket.SOCK_STREAM  # pylint: disable=no-member
            )
            self.sock.settimeout(self._connection_timeout)
            self.sock.connect(self.unix_socket)
        except IOError as err:
            raise InterfaceError(
                errno=2002, values=(self.get_address(), _strioerror(err))
            ) from err
        except Exception as err:
            raise InterfaceError(str(err)) from err

    def switch_to_ssl(
        self, *args: Any, **kwargs: Any  # pylint: disable=unused-argument
    ) -> None:
        """Switch the socket to use SSL."""
        warnings.warn(
            "SSL is disabled when using unix socket connections",
            Warning,
        )


class MySQLTCPSocket(BaseMySQLSocket):
    """MySQL socket class using TCP/IP

    Opens a TCP/IP connection to the MySQL Server.
    """

    def __init__(
        self, host: str = "127.0.0.1", port: int = 3306, force_ipv6: bool = False
    ) -> None:
        super().__init__()
        self.server_host: str = host
        self.server_port: int = port
        self.force_ipv6: bool = force_ipv6
        self._family: int = 0

    def get_address(self) -> str:
        return f"{self.server_host}:{self.server_port}"

    def open_connection(self) -> None:
        """Open the TCP/IP connection to the MySQL server"""
        # pylint: disable=no-member
        # Get address information
        addrinfo: Union[
            Tuple[None, None, None, None, None],
            Tuple[
                socket.AddressFamily,
                socket.SocketKind,
                int,
                str,
                Union[Tuple[str, int], Tuple[str, int, int, int]],
            ],
        ] = (None, None, None, None, None)
        try:
            addrinfos = socket.getaddrinfo(
                self.server_host,
                self.server_port,
                0,
                socket.SOCK_STREAM,
                socket.SOL_TCP,
            )
            # If multiple results we favor IPv4, unless IPv6 was forced.
            for info in addrinfos:
                if self.force_ipv6 and info[0] == socket.AF_INET6:
                    addrinfo = info
                    break
                if info[0] == socket.AF_INET:
                    addrinfo = info
                    break
            if self.force_ipv6 and addrinfo[0] is None:
                raise InterfaceError(f"No IPv6 address found for {self.server_host}")
            if addrinfo[0] is None:
                addrinfo = addrinfos[0]
        except IOError as err:
            raise InterfaceError(
                errno=2003, values=(self.get_address(), _strioerror(err))
            ) from err
        else:
            (self._family, socktype, proto, _, sockaddr) = addrinfo

        # Instanciate the socket and connect
        try:
            self.sock = socket.socket(self._family, socktype, proto)
            self.sock.settimeout(self._connection_timeout)
            self.sock.connect(sockaddr)
        except IOError as err:
            raise InterfaceError(
                errno=2003,
                values=(
                    self.server_host,
                    self.server_port,
                    _strioerror(err),
                ),
            ) from err
        except Exception as err:
            raise OperationalError(str(err)) from err
