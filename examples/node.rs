use std::time::Duration;

use failover::Node;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let name = std::env::args().nth(1).unwrap();
    let l1 = spawn_leader(name.to_string());
    l1.await;
    Ok(())
}

async fn spawn_leader(peer_name: String) {
    tracing::info!(peer_name, "spawning peer");

    let config = failover::Config {
        etcd_options: None,
        lease_time: Duration::from_secs(10),
        prefix: "example".to_owned(),
    };

    let client = Node::new(["http://localhost:2379".to_string()], &peer_name, config)
        .await
        .unwrap();

    let commited = client
        .get_committed(-1, 1337)
        .await
        .unwrap()
        .unwrap_or_default();
    println!("Commited: {:?}", commited);

    tracing::info!("pre commit");
    for i in 0..500_000 {
        if client.commit(-1, 1337, i).await.unwrap() {
            println!("{} {}", peer_name, i);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }else {
            println!("not leader {i} {peer_name}");
        }
    }
    tracing::info!("{} is done", peer_name);
}
