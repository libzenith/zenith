import ssl

import pytest
import websockets
from fixtures.neon_fixtures import NeonProxy


@pytest.mark.asyncio
async def test_websockets(static_proxy: NeonProxy):
    static_proxy.safe_psql("create user ws_auth with password 'ws' superuser")

    user = "ws_auth"
    password = "ws"

    version = b"\x00\x03\x00\x00"
    params = {
        "user": user,
        "database": "postgres",
        "client_encoding": "UTF8",
    }

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.load_verify_locations(str(static_proxy.test_output_dir / "proxy.crt"))

    async with websockets.connect(
        f"wss://{static_proxy.domain}:{static_proxy.external_http_port}/sql",
        ssl=ssl_context,
    ) as websocket:
        startup_message = bytearray(version)
        for key, value in params.items():
            startup_message.extend(key.encode("ascii"))
            startup_message.extend(b"\0")
            startup_message.extend(value.encode("ascii"))
            startup_message.extend(b"\0")
        startup_message.extend(b"\0")
        length = (4 + len(startup_message)).to_bytes(4, byteorder="big")

        await websocket.send([length, startup_message])

        startup_response = await websocket.recv()
        assert startup_response[0:1] == b"R", "should be authentication message"
        assert startup_response[1:5] == b"\x00\x00\x00\x08", "should be 8 bytes long message"
        assert startup_response[5:9] == b"\x00\x00\x00\x03", "should be cleartext"

        auth_message = password.encode("utf-8") + b"\0"
        length = (4 + len(auth_message)).to_bytes(4, byteorder="big")
        await websocket.send([b"p", length, auth_message])

        auth_response = await websocket.recv()
        assert auth_response[0:1] == b"R", "should be authentication message"
        assert auth_response[1:5] == b"\x00\x00\x00\x08", "should be 8 bytes long message"
        assert auth_response[5:9] == b"\x00\x00\x00\x00", "should be authenticated"

        query_message = "SELECT 1".encode("utf-8") + b"\0"
        length = (4 + len(query_message)).to_bytes(4, byteorder="big")
        await websocket.send([b"Q", length, query_message])

        query_response = await websocket.recv()
        # 'T\x00\x00\x00!\x00\x01?column?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x17\x00\x04\xff\xff\xff\xff\x00\x00'
        # 'D\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x011'
        # 'C\x00\x00\x00\rSELECT 1\x00'
        # 'Z\x00\x00\x00\x05I'

        assert query_response[0:1] == b"T", "should be row description message"
        row_description_len = int.from_bytes(query_response[1:5], byteorder="big") + 1
        row_description, query_response = (
            query_response[:row_description_len],
            query_response[row_description_len:],
        )
        assert row_description[5:7] == b"\x00\x01", "should have 1 column"
        assert row_description[7:16] == b"?column?\0", "column should be named ?column?"
        assert row_description[22:26] == b"\x00\x00\x00\x17", "column should be an int4"

        assert query_response[0:1] == b"D", "should be data row message"
        data_row_len = int.from_bytes(query_response[1:5], byteorder="big") + 1
        data_row, query_response = query_response[:data_row_len], query_response[data_row_len:]
        assert (
            data_row == b"D\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x011"
        ), "should contain 1 column with text value 1"

        assert query_response[0:1] == b"C", "should be command complete message"
        command_complete_len = int.from_bytes(query_response[1:5], byteorder="big") + 1
        command_complete, query_response = (
            query_response[:command_complete_len],
            query_response[command_complete_len:],
        )
        assert command_complete == b"C\x00\x00\x00\x0dSELECT 1\0"

        assert query_response[0:6] == b"Z\x00\x00\x00\x05I", "should be ready for query (idle)"

        # close
        await websocket.send(b"X\x00\x00\x00\x04")
        await websocket.wait_closed()


@pytest.mark.asyncio
async def test_websockets_pipelined(static_proxy: NeonProxy):
    """
    Test whether we can send the startup + auth + query all in one go
    """

    static_proxy.safe_psql("create user ws_auth with password 'ws' superuser")

    user = "ws_auth"
    password = "ws"

    version = b"\x00\x03\x00\x00"
    params = {
        "user": user,
        "database": "postgres",
        "client_encoding": "UTF8",
    }

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.load_verify_locations(str(static_proxy.test_output_dir / "proxy.crt"))

    async with websockets.connect(
        f"wss://{static_proxy.domain}:{static_proxy.external_http_port}/sql",
        ssl=ssl_context,
    ) as websocket:
        startup_message = bytearray(version)
        for key, value in params.items():
            startup_message.extend(key.encode("ascii"))
            startup_message.extend(b"\0")
            startup_message.extend(value.encode("ascii"))
            startup_message.extend(b"\0")
        startup_message.extend(b"\0")
        length0 = (4 + len(startup_message)).to_bytes(4, byteorder="big")

        auth_message = password.encode("utf-8") + b"\0"
        length1 = (4 + len(auth_message)).to_bytes(4, byteorder="big")
        query_message = "SELECT 1".encode("utf-8") + b"\0"
        length2 = (4 + len(query_message)).to_bytes(4, byteorder="big")
        await websocket.send(
            [length0, startup_message, b"p", length1, auth_message, b"Q", length2, query_message]
        )

        startup_response = await websocket.recv()
        assert startup_response[0:1] == b"R", "should be authentication message"
        assert startup_response[1:5] == b"\x00\x00\x00\x08", "should be 8 bytes long message"
        assert startup_response[5:9] == b"\x00\x00\x00\x03", "should be cleartext"

        auth_response = await websocket.recv()
        assert auth_response[0:1] == b"R", "should be authentication message"
        assert auth_response[1:5] == b"\x00\x00\x00\x08", "should be 8 bytes long message"
        assert auth_response[5:9] == b"\x00\x00\x00\x00", "should be authenticated"

        query_response = await websocket.recv()
        # 'T\x00\x00\x00!\x00\x01?column?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x17\x00\x04\xff\xff\xff\xff\x00\x00'
        # 'D\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x011'
        # 'C\x00\x00\x00\rSELECT 1\x00'
        # 'Z\x00\x00\x00\x05I'

        assert query_response[0:1] == b"T", "should be row description message"
        row_description_len = int.from_bytes(query_response[1:5], byteorder="big") + 1
        row_description, query_response = (
            query_response[:row_description_len],
            query_response[row_description_len:],
        )
        assert row_description[5:7] == b"\x00\x01", "should have 1 column"
        assert row_description[7:16] == b"?column?\0", "column should be named ?column?"
        assert row_description[22:26] == b"\x00\x00\x00\x17", "column should be an int4"

        assert query_response[0:1] == b"D", "should be data row message"
        data_row_len = int.from_bytes(query_response[1:5], byteorder="big") + 1
        data_row, query_response = query_response[:data_row_len], query_response[data_row_len:]
        assert (
            data_row == b"D\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x011"
        ), "should contain 1 column with text value 1"

        assert query_response[0:1] == b"C", "should be command complete message"
        command_complete_len = int.from_bytes(query_response[1:5], byteorder="big") + 1
        command_complete, query_response = (
            query_response[:command_complete_len],
            query_response[command_complete_len:],
        )
        assert command_complete == b"C\x00\x00\x00\x0dSELECT 1\0"

        assert query_response[0:6] == b"Z\x00\x00\x00\x05I", "should be ready for query (idle)"

        # close
        await websocket.send(b"X\x00\x00\x00\x04")
        await websocket.wait_closed()
