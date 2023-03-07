use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::signal::unix::SignalKind;
use tracing_subscriber::EnvFilter;

use failover::Node;

// #[tokio::main(worker_threads = 1)]
#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::fmt().pretty().with_env_filter(EnvFilter::from_default_env()).init();
    let name = std::env::args().nth(1).unwrap();
    let l1 = spawn_leader(name.to_string());
    l1.await;
    Ok(())
}

async fn spawn_leader(peer_name: String) {
    let to_bysy_loop = spawn_signal_handler();

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
        while to_bysy_loop.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_secs(1));
        }
        if client.commit(-1, 1337, i).await.unwrap() {
            println!("{} {}", peer_name, i);
            tokio::time::sleep(Duration::from_millis(500)).await;
        } else {
            println!("not leader {i} {peer_name}");
        }
    }
    tracing::info!("{} is done", peer_name);
}

fn spawn_signal_handler() -> Arc<AtomicBool> {
    let flag = Arc::new(AtomicBool::new(false));

    {
        let flag = flag.clone();

        std::thread::spawn(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let mut listener =  tokio::signal::unix::signal(SignalKind::from_raw(20)).unwrap();
                loop {
                    listener.recv().await;
                    let prev = flag.load(Ordering::Relaxed);
                    flag.store(!prev, Ordering::Relaxed);
                }
            });
        });
    }

    flag
}