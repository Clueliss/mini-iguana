use clap::Parser;
use futures_util::StreamExt;
use reqwest::{
    Url,
};
use std::{path::PathBuf, time::Instant};
use std::time::Duration;
use reqwest::header::{HeaderName, HeaderValue};

#[derive(Parser)]
struct Opts {
    #[clap(short = 'j', long, default_value_t = 16)]
    worker_threads: usize,

    #[clap(short = 'q', long, default_value_t = 2)]
    query_mixes: u64,

    queries_file: PathBuf,
    endpoint: Url,
}

async fn send_request(client: &reqwest::Client, endpoint: &Url, query: &str) -> anyhow::Result<usize> {
    let resp = client
        .get(endpoint.clone())
        .header(HeaderName::from_static("content-type"), HeaderValue::from_static("application/sparql-query"))
        .header(HeaderName::from_static("accept"), HeaderValue::from_static("application/sparql-results+json"))
        .query(&[("query", query)])
        .send()
        .await?;

    resp.error_for_status_ref()?;

    let mut sz = 0;

    let mut results_stream = resp.bytes_stream();
    while let Some(chunk) = results_stream.next().await {
        sz += chunk?.len();
    }

    Ok(sz)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts: Opts = Opts::parse();

    let queries: Vec<_> = std::fs::read_to_string(opts.queries_file)?
        .lines()
        .map(ToOwned::to_owned)
        .collect();

    let (tx, mut rx) = tokio::sync::mpsc::channel(opts.worker_threads * 2);

    let jhs: Vec<_> = (0..opts.worker_threads)
        .map(move |id| {
            println!("starting worker {id}");

            let queries = queries.clone();
            let endpoint = opts.endpoint.clone();
            let tx = tx.clone();

            tokio::task::spawn(async move {
                let client = reqwest::Client::new();

                for qm in 0..opts.query_mixes {
                    let start = Instant::now();

                    for q in &queries {
                        while let Err(e) = send_request(&client, &endpoint, q).await {
                            tx.send(Err((id, e))).await.unwrap();
                        }
                    }

                    let end = Instant::now();
                    tx.send(Ok((id, qm, end - start))).await.unwrap();
                }
            })
        })
        .collect();

    let mut time = Duration::from_secs(0);
    let mut n_qm = 0;

    while let Some(msg) = rx.recv().await {
        match msg {
            Ok((id, qm, dur)) => {
                n_qm += 1;
                time += dur;
                println!("worker {id} finished query mix {qm} in {}s", dur.as_secs_f32());
            },
            Err((id, err)) => eprintln!("worker {id} received error {err:?}"),
        }
    }

    futures_util::future::join_all(jhs)
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;

    let qmph = n_qm as f64 / (time.as_secs_f64() / 60.0 / 60.0);
    println!("QMPH: {qmph}");

    Ok(())
}
