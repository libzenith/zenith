use super::*;

#[test]
fn test_node_metadata_v1_backward_compatibilty() {
    let v1 = serde_json::to_vec(&serde_json::json!({
        "host": "localhost",
        "port": 23,
        "http_host": "localhost",
        "http_port": 42,
    }));

    assert_eq!(
        serde_json::from_slice::<NodeMetadata>(&v1.unwrap()).unwrap(),
        NodeMetadata {
            postgres_host: "localhost".to_string(),
            postgres_port: 23,
            http_host: "localhost".to_string(),
            http_port: 42,
            use_https: false,
            other: HashMap::new(),
        }
    )
}

#[test]
fn test_node_metadata_v2_backward_compatibilty() {
    let v1 = serde_json::to_vec(&serde_json::json!({
        "host": "localhost",
        "port": 23,
        "http_host": "localhost",
        "http_port": 42,
        "use_https": true,
    }));

    assert_eq!(
        serde_json::from_slice::<NodeMetadata>(&v1.unwrap()).unwrap(),
        NodeMetadata {
            postgres_host: "localhost".to_string(),
            postgres_port: 23,
            http_host: "localhost".to_string(),
            http_port: 42,
            use_https: true,
            other: HashMap::new(),
        }
    )
}
