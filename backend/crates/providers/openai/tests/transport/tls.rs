use std::collections::BTreeMap;

use bytes::{Buf as _, Bytes};

use super::*;

// Captured from Desktop 26.901.51231 / Core 0.153.4 on 2026-09-06.
// Compare wire semantics: rustls randomizes extension order and ephemeral key bytes.
#[derive(Debug, PartialEq, Eq)]
struct ClientHello {
    cipher_suites: Vec<u16>,
    extensions: Vec<u16>,
    groups: Vec<u16>,
    signature_algorithms: Vec<u16>,
    key_shares: Vec<(u16, usize)>,
    alpn: Vec<String>,
}

#[tokio::test]
async fn http_client_hello_should_match_official_rustls_transport() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "https://localhost:{}/",
        listener.local_addr().unwrap().port()
    );
    let client = provider_openai::transport::build_reqwest_client().unwrap();
    let (hello, response) = timeout(Duration::from_secs(10), async {
        tokio::join!(read_client_hello(listener), client.get(url).send())
    })
    .await
    .expect("HTTP ClientHello within timeout");
    assert!(
        response.is_err(),
        "capture endpoint rejects TLS after ClientHello"
    );

    let mut expected = official_websocket_hello();
    expected.extensions.insert(5, 16);
    expected.alpn = vec!["h2".to_owned(), "http/1.1".to_owned()];
    assert_eq!(hello, expected);
}

#[tokio::test]
async fn websocket_client_hello_should_match_official_rustls_transport() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("wss://localhost:{}/", listener.local_addr().unwrap().port());
    let connector =
        provider_openai::transport::tls::maybe_build_rustls_client_config_with_custom_ca()
            .unwrap()
            .map(tokio_tungstenite::Connector::Rustls);
    let (hello, response) = timeout(Duration::from_secs(10), async {
        tokio::join!(
            read_client_hello(listener),
            tokio_tungstenite::connect_async_tls_with_config(url, None, false, connector)
        )
    })
    .await
    .expect("WebSocket ClientHello within timeout");
    assert!(
        response.is_err(),
        "capture endpoint rejects TLS after ClientHello"
    );
    assert_eq!(hello, official_websocket_hello());
}

fn official_websocket_hello() -> ClientHello {
    ClientHello {
        cipher_suites: vec![
            4866, 4865, 4867, 49196, 49195, 52393, 49200, 49199, 52392, 255,
        ],
        extensions: vec![0, 5, 10, 11, 13, 23, 35, 43, 45, 51],
        groups: vec![4588, 29, 23, 24],
        signature_algorithms: vec![1283, 1027, 1539, 2055, 2054, 2053, 2052, 1537, 1281, 1025],
        key_shares: vec![(4588, 1216), (29, 32)],
        alpn: Vec::new(),
    }
}

async fn read_client_hello(listener: TcpListener) -> ClientHello {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut header = [0; 5];
    stream.read_exact(&mut header).await.unwrap();
    assert_eq!(header[0], 22, "TLS handshake record");
    let length = usize::from(u16::from_be_bytes([header[3], header[4]]));
    assert!(length <= 16_384, "bounded ClientHello record");
    let mut record = vec![0; length];
    stream.read_exact(&mut record).await.unwrap();
    stream.write_all(&[21, 3, 3, 0, 2, 2, 40]).await.unwrap();

    let mut hello = Bytes::from(record);
    assert_eq!(hello.get_u8(), 1, "ClientHello message");
    hello.advance(3 + 2 + 32); // Message length, legacy version, random.
    let session_id_len = usize::from(hello.get_u8());
    hello.advance(session_id_len);
    let cipher_suites = u16_values(take_vector(&mut hello));
    let compression_len = usize::from(hello.get_u8());
    hello.advance(compression_len);
    let mut extensions = take_vector(&mut hello);
    let mut values = BTreeMap::new();
    while extensions.has_remaining() {
        let kind = extensions.get_u16();
        assert!(values.insert(kind, take_vector(&mut extensions)).is_none());
    }

    let mut groups = values[&10].clone();
    let groups = u16_values(take_vector(&mut groups));
    let mut signatures = values[&13].clone();
    let signature_algorithms = u16_values(take_vector(&mut signatures));
    let mut shares = values[&51].clone();
    let mut shares = take_vector(&mut shares);
    let mut key_shares = Vec::new();
    while shares.has_remaining() {
        let group = shares.get_u16();
        key_shares.push((group, take_vector(&mut shares).len()));
    }
    let mut alpn = Vec::new();
    if let Some(protocols) = values.get(&16) {
        let mut protocols = protocols.clone();
        let mut protocols = take_vector(&mut protocols);
        while protocols.has_remaining() {
            let length = usize::from(protocols.get_u8());
            alpn.push(String::from_utf8(protocols.split_to(length).to_vec()).unwrap());
        }
    }
    ClientHello {
        cipher_suites,
        extensions: values.into_keys().collect(),
        groups,
        signature_algorithms,
        key_shares,
        alpn,
    }
}

fn take_vector(bytes: &mut Bytes) -> Bytes {
    let length = usize::from(bytes.get_u16());
    bytes.split_to(length)
}

fn u16_values(mut bytes: Bytes) -> Vec<u16> {
    let mut values = Vec::new();
    while bytes.has_remaining() {
        values.push(bytes.get_u16());
    }
    values
}
